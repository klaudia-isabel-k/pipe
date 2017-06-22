
import emails as e
import db
import numpy as np
import pandas as pd

class Error(Exception):
    pass

def remove_old_alerts(df):
    # Choose the list of subscription ids to find latest alerts for
    ids = list(set(df['users_subscription_id']))

    # Create a new list to store ids for latest alerts
    top_ids = []

    # Go through each subscription id
    for i in ids:

        # Choose rows from the dataframe that relates to the subscription id
        rows_for_id = df[df['users_subscription_id'] == i]

        # Select the latest time
        last_alert = rows_for_id['date_insert'].max()

        # Filter the data to only have rows for latest insert time
        rows_for_id = rows_for_id[rows_for_id['date_insert'] == last_alert]

        # Select the minimum
        top_id = rows_for_id['users_subscription_id'].idxmin()

        # Add the first id for that date
        top_ids.append(top_id)

    # Filter dataframe for only the latest ids
    df = df.loc[top_ids]

    # Return the dataframe
    return df

def log_alert(s,date_t,history_flag):
    date_start = date_t[:11] + '09:00:00'

    # If alert history does not exists sent out an email
    if history_flag == 0:
        send_alert_flag = 1
        log_alert_flag = 1

    # Check if there wre any alert sent out lately
    elif str(s['alert_sent']) == "nan":
        send_alert_flag = 1
        log_alert_flag = 1

    # Check if the alert sent an automated email before today's start of day
    elif str(s['date_insert']) < str(date_start):
        if s['alert_sent'] == 1:
            send_alert_flag = 0
            log_alert_flag = 1
        else:
            send_alert_flag = 1
            log_alert_flag = 1
    else:
        send_alert_flag = 0
        log_alert_flag = 0

    # If required store alert in the history
    if log_alert_flag == 1:
        dbmgr.insert_table("Alert_History","{},{},{},{},0,''".format(s['users_subscription_id'],s['today_calc'],s['today_benchmark'],send_alert_flag),date_t)

    # If required sent out an email to the user
    if send_alert_flag == 1:
        e.main(s['user_login'], s['security'], s['benchmark'], s['calc_id'], s['period'], s['today_calc'], s['today_benchmark'])

def calc_concat(security_history_values,s):
    # Calculate the benchmark for a row in a dataframe
    s_add = pd.Series(calc_id_valuation(security_history_values,s), index=['alert_flag','today_calc','today_benchmark'])
    # Merge the existing timeseries with new values
    s = s.append(s_add)
    return s

def calc_id_valuation(security_history_values,s):
    # Calculate today's benchmark fo appropriate calculation type

    # Assign values from series
    calc_id = s['calc_id']
    period = s['period']
    benchmark = s['benchmark']

    # Calculation for % movement
    if calc_id == 1:
        # Calculate the percentage movement between today's value and the specified lookback
        calc = abs(security_history_values[period] / max(security_history_values[0],0.00000001))
        benchmark_t = benchmark

    elif calc_id == 2:
    # Calculation for std movement
        # Sort historical data in ascending order, dates are indexed in opposite direction
        security_history_values = security_history_values.sort_index(axis=0,ascending=False)
        # Calculate log returns
        log_return_values = log_returns(security_history_values)
        # Calculate the rolling standard deviation for period between yesterday's value and the lookback value
        rolling_std_return = log_return_values[1:-1].rolling(period-2).std()[1]
        # Multiply standard deviation by 2 and assign it to today's benchmark
        benchmark_t = rolling_std_return * benchmark
        # Assign today's return value to today's calc
        calc = abs(log_return_values[0])

    else:
        # For calc_ids that do not exist specified dummy value that will not trigger an alert
        calc = np.nan
        benchmark_t = np.nan

    # If today's calculation breached a benchmark, turn alert flag to true
    if calc > benchmark_t:
        flag = True
    else:
        flag = False

    # Return flag, today's calculation and today's benchmark
    return flag,calc,benchmark_t

def log_returns(values):
    # calculate log values in a timeseries dataframe with dates sorted ascending
    return np.log(values) - np.log(values.shift(1))

def main(security, value, date_t, date_alert_cutoff):
    global dbmgr

    try:

        # Connect to database
        dbmgr = db.DatabaseManager("dev.db")

        # Query requested benchmarks for the subscribed security
        subscription = dbmgr.select_df_table('Subscription',
                                                column="*",
                                                orderby=None,
                                                limit=None,
                                                constraint="security = '{}' and active = 1".format(security)
                                                )

        # Check if subscription table returned any results
        if subscription.empty:
            print("No activate subscriptions for '{}' security.".format(security))

        else:

            # Obtain the highest look back value to query an appropriate amount of historical data
            max_lookback = subscription['period'].max()

            # Select historial data for security
            security_history = dbmgr.select_df_price_table(security,
                                                            column="*",
                                                            limit=max_lookback,
                                                            date_to=date_t,
                                                            constraint=None)

            # Check if history table returned any results
            if security_history.empty:
                print("No history prices for '{}' security.".format(security))

            # Check if history table returned enough results
            elif len(security_history['value']) < max_lookback:
                print("Not enough historical prices for subscriptions to '{}' security. Required {} historical data points.".format(security, max_lookback))

            else:
                # Select only relevant columns
                security_history = security_history[['value', 'date_value']]

                # Create a dataframe row with today's value
                security_today = pd.DataFrame([[value,date_t]],columns=['value', 'date_value'])

                # Combine today's value with historical data
                security_history = security_today.append(security_history)

                # Reset index
                security_history.reset_index(drop=True, inplace=True)

                # Calculate Today's benchmark value per required calculation type, benchmark and look back period
                subscription = subscription.apply(lambda y: calc_concat(security_history['value'], y), axis=1)

                # Delete history table as it's no longer required
                del security_history

                # Drop subscriptions that did not trigger an alert
                subscription = subscription[subscription['alert_flag'] == True]

                # Drop columns which are not needed
                subscription.drop(['alert_flag', 'active', 'date_insert'], axis=1, inplace=True)

                # Create a list of subscription ids to query in a database
                subscription_ids = list(subscription['id'])
                sub_id_list = ",".join(str(i) for i in subscription_ids)

                # Rename column
                subscription.rename(columns={'id': 'subscription_id'}, inplace=True)

                # Query Users that subscribe to the prices that triggered an alert
                subscribed_users = dbmgr.select_df_table('Users_Subscription',
                                                        column="*",
                                                        orderby=None,
                                                        limit=None,
                                                        constraint="subscription_id in ({}) and status = 'ACTIVE'".format(sub_id_list)
                                                        )

                # Check if subscribed users table returned any results
                if subscribed_users.empty:
                    print("No subscribed users to subscriptions for '{}' security.".format(security))
                else:
                    # Drop columns which are not needed
                    subscribed_users = subscribed_users.drop(['status','status_approved_by_id','status_approved_date','date_insert'],axis=1)

                    # Join subscriptions and their users
                    active_subscriptions = subscribed_users.merge(subscription, left_on='subscription_id', right_on='subscription_id', how='inner')

                    # Create a list of subscribed users
                    user_sub_ids = list(subscribed_users['id'])
                    user_sub_id_list = ",".join(str(i) for i in user_sub_ids)

                    # Delete dataframes that are no longer needed
                    del subscription, subscribed_users

                    # Query alert history for relevant users and subscriptions
                    alert_history = dbmgr.select_df_table('Alert_History',
                                                            column="users_subscription_id,alert_sent,date_insert",
                                                            orderby='date_insert desc',
                                                            limit=None,
                                                            constraint="users_subscription_id in ({}) and date_insert < '{}' and date_insert > '{}'".format(user_sub_id_list,date_t,date_alert_cutoff)
                                                            )

                    # Check if alert history table returned any results
                    if alert_history.empty:
                        history_flag = 0
                        print("No alert history for subscriptions under {} security.".format(security))

                    else:
                        history_flag = 1
                        # Drop columns which are not needed
                        alert_history.drop(['id'], axis=1, inplace=True)

                        # Keep only the latest alert for each subscription
                        alert_history = remove_old_alerts(alert_history)

                        # Join subscriptions and their history
                        active_subscriptions = active_subscriptions.merge(alert_history, left_on='id', right_on='users_subscription_id', how='left')

                        # Delete dataframes that are no longer needed
                        del alert_history

                        # Drop columns which are not needed
                        active_subscriptions.drop(['users_subscription_id'], axis=1, inplace=True)

                    # Rename column
                    active_subscriptions.rename(columns={'id': 'users_subscription_id'}, inplace=True)

                    # Query Users that subscribe to the prices that triggered an alert
                    users = dbmgr.select_df_table('Users',
                                                            column="user_login",
                                                            orderby=None,
                                                            limit=None,
                                                            constraint="date_to = ''"
                                                            )

                    # Check if users table returned any results
                    if users.empty:
                        print("No stored users in the database.")

                    else:
                        # Join subscriptions and their history
                        active_subscriptions = active_subscriptions.merge(users, left_on='user_id', right_on='id', how='left')

                        # Delete dataframes that are no longer needed
                        del users

                        # Drop columns which are not needed
                        active_subscriptions.drop(['user_id', 'subscription_id', 'id'], axis=1, inplace=True)

                        # Log alerts in history and send when appropriate
                        active_subscriptions.apply(lambda y: log_alert(y, date_t, history_flag), axis=1)

    except Error as ex:
        print ("Error in event handler:", ex)
