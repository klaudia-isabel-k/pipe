
import db

def calc(security,value,date_t,history_dates):
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

    #Calculate Today's benchmark value per required calculation type, benchmark and look back period
    subscription['calc_today'] = subscription.apply(lambda y:
                                        # % movement
                                        abs(security_history['value'][y['period']-1] / max(value,0.00000001))
                                                    # calculation config 1
                                                    if y['calc_id'] == 1
                                                    else
                                        # std movement
                                        (security_history['value'][0:(y['period']+1)].std() * y['benchmark'])
                                                    # calculation config 2
                                                    if y['calc_id'] == 2
                                                    else 0
                                                       , axis=1)

    print(security_history['value'])

    #Calculate if today's benchmark value reached the benchmark trigger
    subscription['alert_flag'] = subscription.apply(lambda y:
                                        # % movement
                                        ((y['calc_today'] > y['benchmark']) and (y['calc_id'] == 1))
                                        # std movement
                                        or #(
                                        (y['calc_today'] < abs(value - security_history['value'][0]) and (y['calc_id'] == 2))
                                        , axis=1)

    #Drop subscriptions that did not trigger an alert
    subscription = subscription[subscription['alert_flag'] == True]

    #Drop columns which are not needed
    subscription = subscription.drop(['alert_flag','active','date_insert'],axis=1)

    #Create a list of subscription ids to query in a database
    subscription_ids = list(subscription['id'])
    sub_id_list = ",".join(str(i) for i in subscription_ids)

    #Rename column
    subscription.rename()

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
    active_subscriptions = subscribed_users.merge(subscription, left_on='subscription_id',right_on='id',how='inner')

    #Query alert history for relevant users and subscriptions
    alert_history = dbmgr.select_df_table('Alert_History',
                                            column="*",
                                            orderby='date_insert desc',
                                            limit=None,
                                            constraint="subscription_id in ({}) and user_id in ({}) and date_insert < '{}' and date_insert > '{}'".format(sub_id_list,user_id_list,date_t,history_dates[-1])
                                            )

    if not alert_history.empty:
        pass

    print(subscribed_users)

    # subscription_id = 1
    #
    # date_count = 1
    # for date in history_dates:
    #     if date_count = 1:
    #         alert_history = dbmgr.select_df_table('Alert_History',
    #                                                 column="*",
    #                                                 orderby='date_insert desc',
    #                                                 limit=None,
    #                                                 constraint="subscription_id = '{}' and date_insert < '{}' and date_insert > '{}'".format(subscription_id,date_t,date_t)
    #                                                 )
