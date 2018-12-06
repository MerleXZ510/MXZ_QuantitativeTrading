import sqlite3

conn = sqlite3.connect('taiwan_stock_data_20040211.db')
#股價表
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
#股票殖利率本益比
# conn.execute('''CREATE TABLE IF NOT EXISTS tw_stock_data_detail (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         date       TEXT    NOT NULL,
#         stockid     TEXT    NOT NULL,
#         name           TEXT    NOT NULL,
#         dividendyield REAL NOT NULL,
#         PER REAL NOT NULL,
#         PBR REAL NOT NULL
#         );''')
conn.close()
