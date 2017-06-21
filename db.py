
import sqlite3
import pandas as pd
import datetime as dt

class DatabaseManager(object): ##Ignore the indentation##
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.c = self.conn.cursor()

    def execute(self, sql):
        self.c.execute(sql)
        self.conn.commit()
        return self.c

    def fetch(self):
        return self.c.fetchall()

    def print_exec(self, sql):
        self.execute(sql)
        print(self.fetch())

    def table_columns(self,table):
        ls = ['id']
        info = self.execute("PRAGMA table_info('{}')".format(table))
        for row in info:
            ls.append(row[1])
        return ls

    def print_table(self, table, column="*",orderby=None,limit=None,constraint=None):
        print(self.select_table(table,column,orderby,limit,constraint))

    def print_df_table(self, table, column="*",orderby=None,limit=None,constraint=None):
        print(self.select_df_table(table,column,orderby,limit,constraint))

    def select_table(self, table, column="*",orderby=None,limit=None,constraint=None):
        sql = "select rowid,{} from {}".format(column,table)
        if constraint:
            sql = sql + " where {}".format(constraint)
        if orderby:
            sql = sql + " order by {}".format(orderby)
        if limit:
            sql = sql + " limit {}".format(limit)
        self.execute(sql)
        return self.fetch()

    def select_df_table(self, table, column="*",orderby=None,limit=None,constraint=None):
        data = self.select_table(table,column,orderby,limit,constraint)
        df = pd.DataFrame(data)
        if column=="*":
            columns = self.table_columns(table)
        else:
            columns = ("id,"+column).split(",")
        if df.empty:
            print('{} table is empty for specified constraint.'.format(table))
        else:
            df.columns = columns
        return df

    def select_df_price_table(self, security, column="*",limit=None,date_to=None,constraint=None):
        if constraint:
            constraint = "security ='{}' and ".format(security) + constraint
        else:
            constraint = "security ='{}'".format(security)
        if date_to:
            constraint = constraint + " and date_value < '{}'".format(date_to)
        orderby = "date_value desc"
        data = self.select_table('Price_History',column,orderby,limit,constraint)
        df = pd.DataFrame(data)
        if column=="*":
            columns = self.table_columns('Price_History')
        else:
            columns = ("id,"+column).split(",")
        if df.empty:
            print('{} table is empty for specified constraint.'.format(table))
        else:
            df.columns = columns
        return df

    def insert_table(self,table,values):
        sql = "insert into {} values ({},'{}')".format(table,values,dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        self.execute(sql)

    def delete_table(self,table, constraint=None):
        sql = "delete from {}".format(table)
        if constraint:
            sql = sql + ' where {}'.format(constraint)
        self.execute(sql)

    def __del__(self):
        self.conn.close()
