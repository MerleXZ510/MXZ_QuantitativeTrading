import pickle
import time
import pandas as pd
import sqlite3


def create_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS tw_stock_data (
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            stockid     TEXT    NOT NULL,
            name           TEXT    NOT NULL,
            tradenum  INT NOT NULL,
            tradetimes  INT NOT NULL,
            tradeamount  INT NOT NULL,
            open  REAL NOT NULL,
            high  REAL NOT NULL,
            low  REAL NOT NULL,
            close  REAL NOT NULL,
            VWAP REAL NOT NULL
            );''')

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)

    return None


def get_lastdata(conn,stockid):
    c = conn.cursor()
    sql = '''SELECT *  from tw_stock_data WHERE STOCKID = '{}' ORDER BY DATE DESC  LIMIT 1 '''.format(
        stockid)
    cursor = c.execute(sql).fetchall()
    return cursor


def create_stockdata(conn, purchases):
    # sql語法
    sql = '''INSERT INTO tw_stock_data (date,stockid,name,tradenum,tradetimes,tradeamount,open,high,low,close,VWAP) \
                                                    VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
    # 資料串列 purchases
    cur = conn.cursor()
    cur.execute(sql, purchases)

    return cur


def create_stockdatadetail(conn, purchases):
    # sql語法
    sql = '''INSERT INTO tw_stock_data_detail (date,stockid,name,dividendyield,PER,PBR) \
                                                    VALUES (?,?,?,?,?,?)'''
    # 資料串列 purchases
    cur = conn.cursor()
    cur.execute(sql, purchases)

    return cur


f1 = open("allstock_20181126.pkl", "rb")
data_1 = pickle.load(f1)
f1.close()

# f2 = open("stock_detail.pkl", "rb")
# data_2 = pickle.load(f2)
# f2.close()

conn = create_connection("taiwan_stock_data_20040211.db")

#建立表格
create_table(conn)
#輸入資料庫
for date in sorted(data_1):
    print(date)
    for stockid in data_1[date].index:
        new_stockid = stockid.replace('"', '').replace('=', '')
        stockdatalist = data_1[date].loc[stockid].values
        #print(date,stockdatalist)
        #有成交 正常輸入
        if stockdatalist[4]!='--':
            purchases = (
            date,
            new_stockid,
            stockdatalist[0],
            eval(stockdatalist[1]),
            eval(stockdatalist[2]),
            eval(stockdatalist[3]),
            eval(stockdatalist[4]),
            eval(stockdatalist[5]),
            eval(stockdatalist[6]),
            eval(stockdatalist[7]),
            eval(stockdatalist[3])/eval(stockdatalist[1]))
            create_stockdata(conn, purchases)
        #未成交 以最高買價為隔日開盤參考價
        elif stockdatalist[10] != '--':
            #分母不能為0
            if eval(stockdatalist[1]) != 0:
                vwap = eval(stockdatalist[3])/eval(stockdatalist[1])
            else :
                vwap = 0
            #stockdatalist[10] 不可使用eval() 會有異常狀況
            purchases = (
            date,
            new_stockid,
            stockdatalist[0],
            eval(stockdatalist[1]),
            eval(stockdatalist[2]),
            eval(stockdatalist[3]),
            stockdatalist[10],
            stockdatalist[10],
            stockdatalist[10],
            stockdatalist[10],
            vwap)

            create_stockdata(conn, purchases)
    conn.commit()

conn.close()













# date = '2009-10-21'

# for stockid in data_1[date].index:
#     stockdatalist = data_1[date].loc[stockid].values
#     print(date, stockid, stockdatalist)
#     #有成交 正常輸入
#     if stockdatalist[4] != '--':
#         purchases = (
#             date,
#             stockid,
#             stockdatalist[0],
#             eval(stockdatalist[1]),
#             eval(stockdatalist[2]),
#             eval(stockdatalist[3]),
#             eval(stockdatalist[4]),
#             eval(stockdatalist[5]),
#             eval(stockdatalist[6]),
#             eval(stockdatalist[7]),
#             eval(stockdatalist[3])/eval(stockdatalist[1]))
#     #未成交 以最高買價為隔日開盤參考價
#     elif stockdatalist[10] != '--':
#         #分母不能為0
#         if eval(stockdatalist[1]) != 0:
#             vwap = eval(stockdatalist[3])/eval(stockdatalist[1])
#         else:
#             vwap = 0
#         purchases = (
#             date,
#             stockid,
#             stockdatalist[0],
#             eval(stockdatalist[1]),
#             eval(stockdatalist[2]),
#             eval(stockdatalist[3]),
#             stockdatalist[10],
#             stockdatalist[10],
#             stockdatalist[10],
#             stockdatalist[10],
#             vwap)
