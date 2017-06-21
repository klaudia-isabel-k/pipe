
# coding: utf-8


import datetime
#import xlwings as xw

from tia.bbg import LocalTerminal

import tia.bbg.datamgr as dm

import numpy as np
import pandas as pd

from collections import defaultdict


def getHist(tickers = ["AAPL US Equity"]):

    fieldsHist = ["SALES_REV_TURN",
                  "EBITDA_ADJUSTED",
                  "EBITDA",
                  "IS_NET_INTEREST_EXPENSE",
                  "IS_INT_EXPENSE",
                  "ARD_OTHER_FINANCIAL_LOSSES",
                  "CF_CASH_FROM_OPER",
                  "CHG_IN_FXD_&_INTANG_AST_DETAILED",
                  "SHORT_AND_LONG_TERM_DEBT",
                  "BS_PENSIONS_LT_LIABS",
                  "PENSION_LIABILITIES",
                  "CASH_AND_MARKETABLE_SECURITIES",
                  "FINANCIAL_SUBSIDIARY_DEBT_&_ADJ",
                  "TANGIBLE_ASSETS",
                  "BS_DEBT_SCHEDULE_YR_2_5",
                  "BS_YEAR_1_PRINCIPAL",
                  "IS_OVER_UNDERFUND_PENSION_EXP",
                  "HISTORICAL MARKET CAP"
    ]



    fieldsRef = ['5Y_MID_CDS_SPREAD',
                 'RTG_MOODY_LT_LC_ISSUER_CREDIT',
                 'RTG_SP_LT_LC_ISSUER_CREDIT',
                 'RTG_FITCH_LT_LC_ISSUER_CREDIT',
                 'INDUSTRY_SUBGROUP',
                 'INDUSTRY_SECTOR',
                 'SECURITY_NAME'
    ]

    respRef = LocalTerminal.get_reference_data(tickers,fieldsRef , ignore_security_error=True,ignore_field_error=True)
    dfRef = respRef.as_frame()
    dfRef.reset_index(inplace=True)
    dfRef.rename(columns={'index': 'Ticker'}, inplace=True)



    #respHist = LocalTerminal.get_historical(tickers,fieldsHist ,ignore_security_error=True,currency='EUR',period_adjustment="FISCAL",period='YEARLY',start='1/1/2004')
    respHist = LocalTerminal.get_historical(tickers,fieldsHist ,ignore_security_error=True,period_adjustment="FISCAL",period='YEARLY',start='1/1/2004')
    sids, frames = respHist.response_map.keys(), respHist.response_map.values()


    if len(respHist.response_map.items())<1:
        return pd.DataFrame()
    dfBBG = pd.concat(frames, keys=sids, axis=0).reset_index()

    dfBBG['Ticker'] =dfBBG['level_0']
    dfBBG.drop('level_0',axis=1,inplace=True)

    fieldsHistEUR = [
                  "EBITDA_ADJUSTED",
                  "EBITDA",
    ]



    respHistEUR = LocalTerminal.get_historical(tickers,fieldsHistEUR ,ignore_security_error=True,currency='EUR',period_adjustment="FISCAL",period='YEARLY',start='1/1/2004')
    sidsEUR, framesEUR = respHistEUR.response_map.keys(), respHistEUR.response_map.values()

    dfBBGEUR = pd.concat(framesEUR, keys=sidsEUR, axis=0).reset_index()
    dfBBGEUR['Ticker'] =dfBBGEUR['level_0']
    dfBBGEUR.drop('level_0',axis=1,inplace=True)
    dfBBGEUR.rename(columns=dict(zip(pd.Series(fieldsHistEUR), pd.Series(fieldsHistEUR)+'_EUR')), inplace=True)

    dfBBG = dfBBG.merge(dfBBGEUR, on=['date','Ticker'],how='left')
    dfBBG['Year'] = dfBBG['date'].dt.year
    dfBBG = dfBBG.groupby('Ticker').apply(calculateMetrics)

    finalData = dfRef.reset_index().merge(dfBBG, how='left',on='Ticker',sort=False).sort('index')
    finalData.rename(columns={'SECURITY_NAME':'Company name','5Y_MID_CDS_SPREAD':'5Y CDS spread',
                              'RTG_MOODY_LT_LC_ISSUER_CREDIT':'MOODYS',
                              'RTG_SP_LT_LC_ISSUER_CREDIT':'S&P',
                              'RTG_FITCH_LT_LC_ISSUER_CREDIT':'FITCH',
                              'INDUSTRY_SUBGROUP':'BLOOMBERG INDUSTRY',
                              'INDUSTRY_SECTOR':'BLOOMBERG SECTOR',
                              },inplace=True)
    return finalData


def calculateMetrics(dfGroup):
    dfGroup['EBITDA to be used in calcs']=dfGroup["EBITDA_ADJUSTED"].apply(lambda x: max(x,0.001))
    dfGroup['EBITDA to be used in calcs'] = dfGroup['EBITDA to be used in calcs'].fillna(dfGroup["EBITDA"])
    dfGroup['EBITDA to be used in calcs_EUR']=dfGroup["EBITDA_ADJUSTED_EUR"].apply(lambda x: max(x,0.001))
    dfGroup['EBITDA to be used in calcs_EUR'] = dfGroup['EBITDA to be used in calcs_EUR'].fillna(dfGroup["EBITDA_EUR"])

    dfGroup['EBITDA Margin to be used in calcs']=dfGroup["EBITDA to be used in calcs"]/dfGroup["SALES_REV_TURN"]
    dfGroup['Interest Expense pre calc']=dfGroup["IS_INT_EXPENSE"].fillna(dfGroup["IS_NET_INTEREST_EXPENSE"])
    dfGroup['Interest Expense to be used in calcs']=dfGroup['Interest Expense pre calc'].fillna(dfGroup["ARD_OTHER_FINANCIAL_LOSSES"])
    dfGroup['Interest Expense to be used in calcs'] = dfGroup['Interest Expense to be used in calcs'].apply(lambda x: max(x,0.1))

    dfGroup["EBITDA: Interest Paid"] = dfGroup['EBITDA to be used in calcs']/dfGroup['Interest Expense to be used in calcs']

    dfGroup['Cashflow from OA - CapEx / EBITDA'] = (dfGroup["CF_CASH_FROM_OPER"]+dfGroup["CHG_IN_FXD_&_INTANG_AST_DETAILED"])/dfGroup["EBITDA to be used in calcs"]

    #dfGroup["Pensions to be used in calcs"] = (dfGroup["PENSION_LIABILITIES"].fillna(dfGroup["BS_PENSIONS_LT_LIABS"])).fillna(dfGroup["IS_OVER_UNDERFUND_PENSION_EXP"])
    dfGroup["Pensions to be used in calcs"] = (dfGroup["PENSION_LIABILITIES"].fillna(dfGroup["BS_PENSIONS_LT_LIABS"]))

    dfGroup["Total Debt incl. Pension"] = dfGroup["SHORT_AND_LONG_TERM_DEBT"]+dfGroup["Pensions to be used in calcs"].fillna(0)

    dfGroup['Net Debt incl. pension (or -net cash)'] = dfGroup['Total Debt incl. Pension']-dfGroup["CASH_AND_MARKETABLE_SECURITIES"]-dfGroup["FINANCIAL_SUBSIDIARY_DEBT_&_ADJ"].fillna(0)

    dfGroup['LTV (Gross Debt & Pen / Tangible Assets)'] = dfGroup['Total Debt incl. Pension']/dfGroup["TANGIBLE_ASSETS"]

    dfGroup['LTV (Gross Debt & Pen / Tangible Assets) (T-1)'] = dfGroup['LTV (Gross Debt & Pen / Tangible Assets)'].iloc[1:].append(pd.Series([np.nan])).reset_index(drop=True)

    dfGroup['LTV (Gross Debt & Pen / Tangible Assets)'] = dfGroup['LTV (Gross Debt & Pen / Tangible Assets)'].fillna(dfGroup['LTV (Gross Debt & Pen / Tangible Assets) (T-1)'])

    dfGroup['Net Debt & Pen : EBITDA'] =  dfGroup['Net Debt incl. pension (or -net cash)']/dfGroup['EBITDA to be used in calcs']

    dfGroup["Total 1-5 Yr Repayments"] =dfGroup[["BS_DEBT_SCHEDULE_YR_2_5","BS_YEAR_1_PRINCIPAL"]].sum(axis=1)

    dfGroup["Enterprise Value"]= dfGroup["HISTORICAL MARKET CAP"] + dfGroup["Net Debt incl. pension (or -net cash)"]


    mostRecentYear = max(dfGroup.Year.values)

    recentDf = dfGroup[(dfGroup.Year == (mostRecentYear))].iloc[0]

    if(mostRecentYear-1) in dfGroup.Year.values:
        recentDfMinusOne  =dfGroup[(dfGroup.Year == (mostRecentYear-1))].iloc[0]
    else:
        recentDfMinusOne=  recentDf

    if pd.isnull(recentDf.SALES_REV_TURN):
        mostRecentSalesRev= recentDfMinusOne.SALES_REV_TURN
    else:
        mostRecentSalesRev= recentDf.SALES_REV_TURN

    dfGroup["Revenues"]= mostRecentSalesRev

    try:
        TenYearPriorSalesRev = dfGroup[dfGroup.Year == (mostRecentYear-10)].iloc[0].SALES_REV_TURN
        tenYrRevenueCAGR = pow(mostRecentSalesRev/TenYearPriorSalesRev,1/10)-1
    except:
        tenYrRevenueCAGR= np.nan

    dfGroup['10yr Revenue CAGR'] =  tenYrRevenueCAGR

    mostRecentYear = max(dfGroup.Year.values)

    if pd.isnull(recentDf["EBITDA Margin to be used in calcs"]):
        EbitdaMargin = recentDfMinusOne["EBITDA Margin to be used in calcs"]
    else:
        EbitdaMargin =  recentDf["EBITDA Margin to be used in calcs"]


    dfGroup['EBITDA Margin'] = EbitdaMargin


    if pd.isnull(recentDf['EBITDA to be used in calcs_EUR']):
        EbitdaAdjusted =recentDfMinusOne['EBITDA to be used in calcs_EUR']
    else:
        EbitdaAdjusted =  recentDf['EBITDA to be used in calcs_EUR']


    dfGroup["Adj. EBITDA"]=EbitdaAdjusted

    if pd.isnull(recentDf["EBITDA Margin to be used in calcs"]) and ((mostRecentYear-1) in dfGroup.Year.values):
        mostRecentYear -= 1

    EbitdaMarginData = dfGroup[(dfGroup.Year<=mostRecentYear) &  (dfGroup.Year>=(mostRecentYear-9))]["EBITDA Margin to be used in calcs"].values
    EbitdaMarginMean = np.nanmean(EbitdaMarginData)
    EbitdaStDev =  np.nanstd(EbitdaMarginData)
    dfGroup['EBITDA Margin Mean'] = EbitdaMarginMean
    dfGroup['EBITDA Margin StDev'] = EbitdaStDev

    EbitdaMarginStability=  (EbitdaStDev/EbitdaMarginMean) if  ((EbitdaStDev/EbitdaMarginMean)>0) else 1

    dfGroup['EBITDA Margin Stability'] =  EbitdaMarginStability

    recentDfSum = dfGroup[(dfGroup.Year >= (mostRecentYear-2))].fillna(0)
    cashFlowSum= np.sum(recentDfSum[["CF_CASH_FROM_OPER","CHG_IN_FXD_&_INTANG_AST_DETAILED"]].sum(axis=0).values)
    EbitdaMarginSum= recentDfSum[["EBITDA to be used in calcs"]].sum(axis=0).values[0]

    Av3yrCashConversion =  max((cashFlowSum/EbitdaMarginSum),0)

    dfGroup['Av. 3yr Cash Conversion'] = Av3yrCashConversion

    ebidtaToInterest = np.nan
    if pd.notnull(recentDf["EBITDA to be used in calcs"]):
        if max(50,recentDf["EBITDA: Interest Paid"])>0.1:
            ebidtaToInterest = min(50,recentDf["EBITDA: Interest Paid"])
    else :
        if max(50,recentDfMinusOne["EBITDA: Interest Paid"])>0.1:
            ebidtaToInterest = min(50,recentDfMinusOne["EBITDA: Interest Paid"])


    dfGroup['EBITDA to Interest Paid'] = ebidtaToInterest

    mostRecentYear = max(dfGroup.Year.values)

    netDebtPenMostRecent = recentDf["Net Debt & Pen : EBITDA"]

    netDebtPenMostRecentMinusOne = recentDfMinusOne["Net Debt & Pen : EBITDA"]

    if (pd.notnull(netDebtPenMostRecent) and (netDebtPenMostRecent>9.99)):
        netDebtPenEBITDA= np.nan
    elif  (pd.notnull(netDebtPenMostRecent) and (netDebtPenMostRecent<0)):
        netDebtPenEBITDA = 0
    elif (pd.notnull(netDebtPenMostRecent) and (netDebtPenMostRecent<9.99) and (netDebtPenMostRecent>0)):
        netDebtPenEBITDA = netDebtPenMostRecent
    else:
        netDebtPenEBITDA =  netDebtPenMostRecentMinusOne

    dfGroup['Net Debt & Pen:EBITDA'] = netDebtPenEBITDA

#
#    recentDf = dfGroup[(dfGroup.Year == (mostRecentYear))].iloc[0]
#    if(mostRecentYear-1) in dfGroup.Year.values:
#
#        recentDfMinusOne  =dfGroup[(dfGroup.Year == (mostRecentYear-1))].iloc[0]
#    else:
#        recentDfMinusOne=  recentDf
    netDebtPenEV = (recentDf["Net Debt incl. pension (or -net cash)"]/recentDf["Enterprise Value"]) or (recentDfMinusOne["Net Debt incl. pension (or -net cash)"]/recentDfMinusOne["Enterprise Value"])

    dfGroup['Net Debt & Pen / EV'] = netDebtPenEV

#
#    recentDf = dfGroup[(dfGroup.Year == (mostRecentYear))].iloc[0]
#    if(mostRecentYear-1) in dfGroup.Year.values:
#
#        recentDfMinusOne  =dfGroup[(dfGroup.Year == (mostRecentYear-1))].iloc[0]
#    else:
#        recentDfMinusOne=  recentDf

    if pd.notnull(recentDf["SHORT_AND_LONG_TERM_DEBT"]*recentDf["Net Debt incl. pension (or -net cash)"]):
        ltv = recentDf["LTV (Gross Debt & Pen / Tangible Assets)"]
    elif pd.notnull(recentDfMinusOne["SHORT_AND_LONG_TERM_DEBT"]*recentDfMinusOne["Net Debt incl. pension (or -net cash)"]):
        ltv = recentDfMinusOne["LTV (Gross Debt & Pen / Tangible Assets)"]
    else:
        ltv = np.nan


    dfGroup['LTV (GD & Pen / Tangibles)'] = ltv

#
#    recentDf = dfGroup[(dfGroup.Year == (mostRecentYear))].iloc[0]
#
#    if(mostRecentYear-1) in dfGroup.Year.values:
#
#        recentDfMinusOne  =dfGroup[(dfGroup.Year == (mostRecentYear-1))].iloc[0]
#    else:
#        recentDfMinusOne=  recentDf

    if pd.notnull(recentDf["Total 1-5 Yr Repayments"]):
        percent_year_one_five_debt_mat  = (recentDf["Total 1-5 Yr Repayments"]/recentDf["SHORT_AND_LONG_TERM_DEBT"])
    else:
        percent_year_one_five_debt_mat = (recentDfMinusOne["Total 1-5 Yr Repayments"]/recentDfMinusOne["SHORT_AND_LONG_TERM_DEBT"])
    dfGroup['% Year 1-5 Debt Mat'] = percent_year_one_five_debt_mat


    return dfGroup


def getComputedMetrics(tickers):




    colsToReturn = ['Ticker',
                    'Company name',
                    '5Y CDS spread',
                    'MOODYS',
                    'S&P',
                    'FITCH',
                    'BLOOMBERG INDUSTRY',
                    'BLOOMBERG SECTOR',
                    '10yr Revenue CAGR','EBITDA Margin','EBITDA Margin Stability',
                    'Av. 3yr Cash Conversion',
                    'EBITDA to Interest Paid','Net Debt & Pen:EBITDA',
                    'Net Debt & Pen / EV','LTV (GD & Pen / Tangibles)','% Year 1-5 Debt Mat',
                    'Revenues','Adj. EBITDA']


    data = getHist(tickers)
    if len(data)>0:
        return data [colsToReturn].drop_duplicates(subset='Ticker',keep='last').reset_index(drop=True)
    else:
        return pd.DataFrame(columns=colsToReturn)
