import numpy as np
import pandas as pd
import MySQLdb
import tushare as ts
import datetime
import time
import tushare as ts
import os
from sqlalchemy import create_engine
# from WindPy import *
# w.start(waitTime=10)#命令超时时间为 120 秒
def formatDate(Date, formatType='YYYYMMDD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType
def stock_financial_data_to_mysql():#保存了2010年以来的财务数据，使用时须修改时间
    engine=create_engine('mysql://root:0325xb@localhost/stocks_financial_data?charset=utf8')
    df=ts.get_stock_basics()
    stocks=df.index.tolist()
    i=0
    j=0
    #for stock in stocks:
    #date=df[df['timeToMarket']>0].min()
    #date.sort(ascending=True)
    #print(date)
    #earliest_date=date[2]

    start_year=2010
    quarter_list=[1,2,3,4]
    #parser.parse(str(date))

    start_quarter=1
    #date=formatDate(str(date),'YYYY-MM-DD')            #改一下格式
    #print(stock,start_month,start_quarter)

    for year in range(start_year,2018):
        for quarter in quarter_list:
            # try:
            df1=ts.get_report_data(year,quarter)
            df1.index = df1['code']
            df2=ts.get_profit_data(year,quarter)
            df3=ts.get_operation_data(year,quarter)
            df4=ts.get_growth_data(year,quarter)
            df5=ts.get_debtpaying_data(year,quarter)
            df6=ts.get_cashflow_data(year,quarter)
            # df1.index = df1['code']
            # df2.index = df2['code']
            # df3.index = df3['code']
            # df4.index = df4['code']
            # df5.index = df5['code']
            # df6.index = df6['code']
            # df=pd.concat([df1,df2],axis=1, join_axes=[df1.index])
            df1_2=pd.merge(df1,df2 ,on='code')
            df3_4=pd.merge(df3,df4 ,on='code')
            df5_6=pd.merge(df5,df6 ,on='code')
            df1234=pd.merge(df1_2,df3_4 ,on='code')
            df=pd.merge(df1234,df5_6 ,on='code')
            df=df.drop_duplicates(['code'])
            df.to_csv("financial_data_%s_%s.csv"%(year,quarter),encoding = "GB18030")
            df.to_sql('financial_data_%s_%s'%(year,quarter),engine,if_exists='append')
                #print(df1)
            # except:
            #     j=j+1
            #     print(j)
            #     #continue
def history_day_k_to_mysql_qfq(start_date = None,end_date = None):
    #stocks:股票列表
    #engine：create_engine object
    #获取所有股票的前复权价格日k
    df_basic=ts.get_stock_basics()
    #df.to_csv("testfoo.csv",encoding = "GB18030")
    df_log=pd.DataFrame()
    df_log=df_log.reset_index()
    df_log['code']=None
    df_log['update_time']=None
    stocks=df_basic.index
    engine=create_engine('mysql://root:0325xb@localhost/stock_day_k_history_data_qfq?charset=utf8')
    table_name=engine.table_names()
    for i in range(len(table_name)):
        table_name[i]=str(table_name[i])[-6:]
    stocks = list(set(stocks) ^ set(table_name))#未获取到的股票=所有股票-已经获取的股票
    print(len(stocks))
    conn=ts.get_apis()
    try:
        df_log_old=pd.read_sql('update_log',engine)
        read_state=True
    except:
        read_state=False#读取状态错误
    i=0
    for stock in stocks:
        Finish=False
        #判断是否为最新日期的价格
        if read_state:
            if str(df_log_old.iloc[stock,'update_time'])[0:10]=='2018-01-04':
                Finish=True
            else:
                Finish=False

        if not Finish:
            try:
                df=ts.bar(stock,conn,start_date,adj='qfq') #前复权
                df['code']=stock
                df=df.reset_index()
                print('success')
                #df.rename(columns={'date':'trade_date'}, inplace = True)
                #df.index=df['trade_date']
                #print(df)
                #engine.execute("CREATE TABLE IF NOT EXISTS stock_day_k_history_data.stock_%s(date varchar(30))"%stock)
                #engine.execute('alter table stock_day_k_history add column date varchar(30) if not exists date')
                #engine.execute(CREATE INDEX ix_stock_603283_date ON stock_603283 (date)
                df.to_sql('stock_%s'%stock,engine,if_exists='replace')
                df_log.ix[i,'code'] =stock
                df_log.ix[i,'update_time'] =df.ix[0,'datetime']
                i=i+1
            except:
                print(stock)
            #
                continue

    df_log.to_sql('update_log_18-01-04',engine,if_exists='replace')
    print('finish')

'''
    dh=ts.get_k_data(stock,ktype = '5',start=date,end='2017-12-31') #不复权
    dg=ts.get_k_data(stock,ktype = '5',start=date,end='2017-12-31',autype='hfq')     #后复权
    del dg['code']
    del dg['date']
    del dg['volume']
    dg.rename(columns={'open':'fopen', 'close':'fclose', 'high':'fhigh', 'low':'flow','volume':'fvolume'}, inplace = True)
    mergeColumn=pd.concat([dh,dg],axis=1)
    print(dh)
    mergeColumn.to_sql('history',engine,if_exists='append')
'''
for i in range(10):
    try:
        history_day_k_to_mysql_qfq()#更新时需要加日期，起始和结束日期
    except:
        print(i)
        time.sleep(10)
        continue