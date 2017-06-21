
#import getBloomData as bd
#bd.getHist()

# import bloomSubscription
# import sendEmail as send
#
# ls = [["klaudia.kloczewiak@partner.commerzbank.com","SHP:LN",2,1,7]]
#
# send.send_email(ls)

import benchmarkCalc as bcCalc
import datetime as dt
import db

dbmgr = db.DatabaseManager("dev.db")


date_today = dt.datetime(2017,6,19,14,0,0)

date_t = date_today.strftime('%Y-%m-%d %H:%M:%S.000')
date_t_24 = (date_today - dt.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
date_t_7 = (date_today - dt.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
date_t_30 = (date_today - dt.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
history_dates = [date_t,date_t_24,date_t_7,date_t_30]
history_dates = sorted(history_dates,reverse=True)

x = bcCalc.main('isin/US0394831020',42,date_t,history_dates)


#dbmgr.print_table("sqlite_master", "*", constraint="type='table' and name = 'Alert_History'")
#dbmgr.print_table("sqlite_master", "name", constraint="type='table'")
#dbmgr.insert_table("Alert_History","2,1,'Test',1")
# dbmgr.execute("update Alert_History set date_insert = '2017-06-19 12:43:00' where rowid = 9 ")
# dbmgr.print_df_table("Alert_History")

# ls = [['16',42.56],['15',42.36],['14',42.05],['13',42.36],['12',42.43],['09',42.02],['08',42.01],['07',41.91],['06',41.95],['05',41.94]]
#
# for r in ls:
#     sql = "INSERT INTO Subscription VALUES ('isin/US0394831020',{},'2017-06-{} 16:00:00.000','2017-06-19 13:37:23.142')".format(r[1],r[0])
#     dbmgr.execute(sql)
