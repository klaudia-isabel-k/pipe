import benchmarkCalc as bcCalc
import datetime as dt
import db

dbmgr = db.DatabaseManager("dev.db")


date_today = dt.datetime(2017,6,18,14,0,2)

date_t = date_today.strftime('%Y-%m-%d %H:%M:%S.000')
date_t_2 = (date_today - dt.timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
date_t_30 = (date_today - dt.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
#history_dates = sorted(history_dates,reverse=True)

x = bcCalc.main('isin/US0394831020',41,date_t,date_t_2)
#'isin/US0394831020'

#dbmgr.print_table("sqlite_master", "*", constraint="type='table' and name = 'Alert_History'")
#dbmgr.print_table("sqlite_master", "name", constraint="type='table'")
# dbmgr.insert_table("Price_History","'isin/US0394831020',41.25,'2017-06-21 16:00:00.000'")
# dbmgr.insert_table("Alert_History","1,5.03,1,0,0,''")
# dbmgr.insert_table("Alert_History","2,5.03,5,1,0,''")
# dbmgr.insert_table("Alert_History","4,0.33,0.22,0,0,''")
# dbmgr.insert_table("Alert_History","4,0.44,0.22,0,0,''")
# dbmgr.insert_table("Alert_History","5,0.44,0.33,0,0,''")
#dbmgr.delete_table("Alert_History")
#dbmgr.execute("Create table Alert_History (users_subscription_id, alert_calc real, alert_benchmark real, alert_sent integer, alert_responded integer, alert_responded_when text, date_insert text)")
#dbmgr.execute("UPDATE Price_History set date_insert = '2017-06-22 13:34:23.142' where rowid = 13")
#
#dbmgr.print_df_table("Alert_History")
# dbmgr.print_df_table("Users_Subscription")

# ls = [['16',42.56],['15',42.36],['14',42.05],['13',42.36],['12',42.43],['09',42.02],['08',42.01],['07',41.91],['06',41.95],['05',41.94]]
#
# for r in ls:
#     sql = "INSERT INTO Subscription VALUES ('isin/US0394831020',{},'2017-06-{} 16:00:00.000','2017-06-19 13:37:23.142')".format(r[1],r[0])
#     dbmgr.execute(sql)
