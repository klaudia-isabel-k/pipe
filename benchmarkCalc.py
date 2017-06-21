
import db
import numpy as np
import pandas as pd

def calc_concat(security_history_values,s):
    s_add = pd.Series(calc_id_valuation(security_history_values,s), index=['alert_flag','today_calc','today_benchmark'])
    s = s.append(s_add)
    return s

def calc_id_valuation(security_history_values,df):

    calc_id = df['calc_id']
    period = df['period']
    benchmark = df['benchmark']

    # Calculation for % movement
    if calc_id == 1:
        #Calculate the percentage movement between today's value and the specified lookback
        calc = abs(security_history_values[period] / max(security_history_values[0],0.00000001))

    elif calc_id == 2:
    # Calculation for std movement
        #Sort historical data in ascending order, dates are indexed in opposite direction
        security_history_values = security_history_values.sort_index(axis=0,ascending=False)
        #Calculate log returns
        log_return_values = log_returns(security_history_values)
        #Calculate the rolling standard deviation for period between yesterday's value and the lookback value
        rolling_std_return = log_return_values[1:-1].rolling(period-2).std()[1]
        #Multiply standard deviation by 2 and assign it to today's benchmark
        benchmark = rolling_std_return * benchmark
        #Assign today's return value to today's calc
        calc = abs(log_return_values[0])

    else:
        #For calc_ids that do not exist specified dummy value that will not trigger an alert
        calc = -0.999999
        benchmark = 0.0000001

    #If today's calculation breached a benchmark, turn alert flag to true
    if calc > benchmark:
        flag = True
    else:
        flag = False

    #Return flag, today's calculation and today's benchmark
    return flag,calc,benchmark

def log_returns(values):
    #calculate log values in a timeseries dataframe with dates sorted ascending
    return np.log(values) - np.log(values.shift(1))

def main(security,value,date_t,history_dates):
    #Connect to database
    dbmgr = db.DatabaseManager("dev.db")

    #Query requested benchmarks for the subscribed security
    subscription = dbmgr.select_df_table('Subscription',
                                            column="*",
                                            orderby=None,
                                            limit=None,
                                            constraint="security = '{}' and active = 1".format(security)
                                            )
    #Obtain the highest look back value to query an appropriate amount of historical data
    max_lookback = subscription['period'].max()

    #Select historial data for security
    security_history = dbmgr.select_df_price_table(security,column="*",limit=max_lookback,date_to=date_t,constraint=None)

    #Select only relevant columns
    security_history = security_history[['value','date_value']]

    #Create a dataframe row with today's value
    security_today = pd.DataFrame([[value,date_t]],columns=['value','date_value'])

    #Combine today's value with historical data
    security_history = security_today.append(security_history)

    #Reset index
    security_history = security_history.reset_index(drop=True)

    #Calculate Today's benchmark value per required calculation type, benchmark and look back period
    subscription = subscription.apply(lambda y: calc_concat(security_history['value'],y), axis=1)

    #Delete history table as it's no longer required
    del security_history

    #Drop subscriptions that did not trigger an alert
    subscription = subscription[subscription['alert_flag'] == True]

    #Drop columns which are not needed
    subscription = subscription.drop(['alert_flag','active','date_insert'],axis=1)

    #Create a list of subscription ids to query in a database
    subscription_ids = list(subscription['id'])
    sub_id_list = ",".join(str(i) for i in subscription_ids)

    #Rename column
    subscription.rename(columns={'id': 'subscription_id'}, inplace=True)

    #Query Users that subscribe to the prices that triggered an alert
    subscribed_users = dbmgr.select_df_table('Users_Subscription',
                                            column="*",
                                            orderby=None,
                                            limit=None,
                                            constraint="subscription_id in ({}) and status = 'ACTIVE'".format(sub_id_list)
                                            )

    #Drop columns which are not needed
    subscribed_users = subscribed_users.drop(['status','status_approved_by_id','status_approved_date','date_insert'],axis=1)

    #Create a list of subscribed users
    user_ids = list(subscribed_users['user_id'])
    user_id_list = ",".join(str(i) for i in user_ids)

    #Join subscriptions and their users
    active_subscriptions = subscribed_users.merge(subscription, left_on='subscription_id',right_on='subscription_id',how='inner')

    #Delete dataframes that are no longer needed
    del subscription, subscribed_users

    #Query alert history for relevant users and subscriptions
    alert_history = dbmgr.select_df_table('Alert_History',
                                            column="*",
                                            orderby='date_insert desc',
                                            limit=None,
                                            constraint="subscription_id in ({}) and user_id in ({}) and date_insert < '{}' and date_insert > '{}'".format(sub_id_list,user_id_list,date_t,history_dates[-1])
                                            )

    if not alert_history.empty:
        pass
