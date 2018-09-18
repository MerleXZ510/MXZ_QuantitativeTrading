import requests
from io import StringIO
import pandas as pd
import numpy as np
import pickle


def crawl_price(date):
    url='http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + str(date).split(' ')[0].replace('-','') + '&type=ALLBUT0999'
    r = requests.post(url)
    stock_dict={ord(' '): None}
    stock_list=[i.translate(stock_dict)   for i in r.text.split('\n')  if  len(i.split('",')) == 17 and i[0] != '=' ]
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

def crawl_detail(date):
    url = 'http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html&date=' + str(date).split(' ')[0].replace('-','') + '&selectType=ALL'
    ret = pd.read_html(url)[0]
    if len(ret.columns)==7:
        ret.columns = ['證券代號','證券名稱','殖利率(%)','股利年度','本益比','股價淨值比','財報年/季']
    elif len(ret.columns)==5:
        ret.columns = ['證券代號','證券名稱','本益比','殖利率(%)','股價淨值比']
    return ret

###爬資料  
import datetime
import time
data = {}
data_detail = {}
n_days = 50
date = datetime.datetime.now()
fail_count = 0
allow_continuous_fail_count = 20
while len(data) < n_days:
    print('parsing', date)
    # 使用 crawPrice 爬資料
    date_ = str(date).split(' ')[0]
    try:
        # 抓資料
        data[date_] = crawl_price(date)
        print('爬取股價成功 1!')
        fail_count = 0
    except:
        # 假日爬不到
        print('爬取股價失敗')
        fail_count += 1
        if fail_count == allow_continuous_fail_count:
            raise
    try:
        data_detail[date_] = crawl_detail(date)
        print('爬取詳細成功 2!')
    except:
        print('爬取詳細失敗')
        pass
    # 減一天
    date -= datetime.timedelta(days=1)
    time.sleep(5)
    
# for i in sorted(data):
#     print(i)
# f = open("allStock.pkl","wb")
# pickle.dump(data,f)
# f.close()    

# f = open("allStock_detail.pkl","wb")
# pickle.dump(data_detail,f)
# f.close()    
