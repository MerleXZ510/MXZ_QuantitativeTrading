import datetime
import sqlite3
import time
from io import StringIO
import numpy as np
import pandas as pd
import requests


#連線
def __create_connection(db_file):
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

#查最後更新檔案
def __get_lastdata(conn, stockid):
    c = conn.cursor()
    sql = '''SELECT *  from tw_stock_data WHERE STOCKID = '{}' ORDER BY DATE DESC  LIMIT 1 '''.format(
        stockid)
    cursor = c.execute(sql).fetchall()
    return cursor

#新增資料(1筆)
def __create_stockdata(conn, purchases):
    # sql語法
    sql = '''INSERT INTO tw_stock_data (date,stockid,name,tradenum,tradetimes,tradeamount,open,high,low,close,VWAP) \
                                                    VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
    # 資料串列 purchases
    cur = conn.cursor()
    cur.execute(sql, purchases)

    return cur

#將每日價格輸入DB(所有)
def __create_stock_date_data(conn, date , data_1):
    for stockid in data_1[date].index:
        new_stockid = stockid.replace('"', '').replace('=', '')
        stockdatalist = data_1[date].loc[stockid].values
        #有成交 正常輸入
        if stockdatalist[4] != '--':
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
            __create_stockdata(conn, purchases)
        #未成交 以最高買價為隔日開盤參考價
        elif stockdatalist[10] != '--':
            #分母不能為0
            if eval(stockdatalist[1]) != 0:
                vwap = eval(stockdatalist[3])/eval(stockdatalist[1])
            else:
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

            __create_stockdata(conn, purchases)
    conn.commit()

#爬取資料輸入DB 預設起始日期
def __update_stockdata(conn, start_date = datetime.datetime.strptime("2004-02-11 17:00:01", "%Y-%m-%d %H:%M:%S")): 
    data = {}
    date = datetime.datetime.now()
    while start_date < date:
        print('parsing', date)
        # 使用 __crawl_price 爬資料
        date_ = str(date).split(' ')[0]
        try:
            # 抓資料
            data[date_] = __crawl_price(date)
            print('爬取股價成功 1!')
        except:
            # 假日爬不到
            print('爬取股價失敗')
        # 減一天
        date -= datetime.timedelta(days=1)
        time.sleep(5)
    for date in sorted(data):
        print('寫入',date)
        #__create_stock_date_data(conn, date, data)
    return None

#爬取資料 輸出dataframe
def __crawl_price(date):
    #http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=20181003&type=ALLBUT0999
    url='http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + str(date).split(' ')[0].replace('-','') + '&type=ALLBUT0999'
    r = requests.post(url)   
    stock_dict={ord(' '): None}
    #stock_list=[i.translate(stock_dict)   for i in r.text.split('\n')  if  len(i.split('",')) == 17 and i[0] != '=' ]
    stock_list=[i.translate(stock_dict)   for i in r.text.split('\n')  if  len(i.split('",')) == 17 and (i[0] != '=' or len(i.split('",')[0]) <= 6)]
    ret = pd.read_csv(StringIO("\n".join(stock_list)), header=0)
    ret = ret.set_index('證券代號')
    ret['成交金額'] = ret['成交金額'].str.replace(',','')
    ret['成交股數'] = ret['成交股數'].str.replace(',','')
    ret['成交筆數'] = ret['成交筆數'].str.replace(',','')
    ret['開盤價'] = ret['開盤價'].str.replace(',','')
    ret['最高價'] = ret['最高價'].str.replace(',','')
    ret['最低價'] = ret['最低價'].str.replace(',','')
    ret['收盤價'] = ret['收盤價'].str.replace(',','') 
    return ret

#方式
def update():
    conn = __create_connection("taiwan_stock_data_20040211.db")
    df = __get_lastdata(conn,'0050')[0][1]
    start_date = datetime.datetime.strptime(df+" 00:00:01", "%Y-%m-%d %H:%M:%S")
    start_date += datetime.timedelta(days=1)
    #print(start_date)
    __update_stockdata(conn, start_date)
    conn.close()


if __name__ == '__main__':
    #建立連線
    conn = __create_connection("taiwan_stock_data_20040211.db")
    df = __get_lastdata(conn,'0050')[0][1]
    start_date = datetime.datetime.strptime(df+" 00:00:01", "%Y-%m-%d %H:%M:%S")
    start_date += datetime.timedelta(days=1)
    #print(start_date)
    __update_stockdata(conn, start_date)
    conn.close()
