import FinanceDataReader as fdr
import ccxt
import datetime
import time
import pandas as pd
import exchangeOption as op
import matplotlib.pyplot as plt

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 


def ohlcv(exchange, symbol, start, end):
    Exchange = op.exchangeOption(exchange)
    
    start=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    start=start-datetime.timedelta(hours=9)   # utc 시간
    
    end = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    end = end-datetime.timedelta(hours=9)
    
    start1=start.strftime("%Y-%m-%d %H:%M:%S")
    since = Exchange.parse8601(start1)

    timestamp=[]
    price=[]
    
    while start<end:
                        
        ohlcv = Exchange.fetch_ohlcv(symbol,'1m',since)
        
        for i in ohlcv:
            timestamp.append(i[0])
            price.append(i[4])
            
        start = start + datetime.timedelta(minutes=len(ohlcv))
        start1=start.strftime("%Y-%m-%d %H:%M:%S")
        since = Exchange.parse8601(start1)

        time.sleep(0.15)
        
    
    ex_df = pd.DataFrame({'timestamp' : timestamp, 'price' : price})
    ex_df['timestamp']=pd.to_datetime(ex_df['timestamp']/1000, unit='s')
    ex_df['timestamp']=ex_df['timestamp']+datetime.timedelta(hours=9)
    ex_df['exchange'] = exchange
    ex_df['symbol'] = symbol
    print(ex_df)
    return ex_df
    
def dfParsing(ex_df1, ex_df2, start):
    
    #2개 거래소 timestamp 동기화
    ex_1 = ex_df1['exchange'][0]
    
    print('start data parsing')
    df = pd.concat([ex_df1,ex_df2])
    df.sort_values('timestamp', ascending=True)
    df.drop_duplicates(['timestamp'], keep=False, inplace = True)
    
    if len(df)>0:
        for i in df.index:
            if df['exchange'][i] == ex_1 :
                ex_df1.drop([i],inplace=True)
    
            else:
                ex_df2.drop([i], inplace=True)

    ex_df1.reset_index(drop=True, inplace=True)
    ex_df2.reset_index(drop=True, inplace=True)

    print(ex_df1)
    print(ex_df2)


    
    #환율데이터 삽입 
    usdkrw = fdr.DataReader('USD/KRW', start)
    usdkrw = usdkrw.reset_index(drop=False)
    
    print(usdkrw)
    
    ex_df1['usdkrw'] = None
    
    usdkrw_index = 0
    for i in ex_df1.index:
        if ex_df1.at[i,'timestamp'].day == usdkrw.at[usdkrw_index,'Date'].day:
            ex_df1.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'Close'])
        
        elif (len(usdkrw)-1) < (usdkrw_index+1) :
            ex_df1.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'Close'])
            
        elif usdkrw.at[usdkrw_index+1,'Date'].day == ex_df1.at[i,'timestamp'].day :
            usdkrw_index += 1
            ex_df1.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'Close'])
        
        else :
            ex_df1.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'Close'])
    
    ex_df1['krw_price'] = ex_df1['price']*ex_df1['usdkrw']

    #Gimp 데이터 추가 
    ex_df1['gimp'] = (ex_df2['price']-ex_df1['krw_price'])/ex_df1['krw_price'] * 100
    
    pd.options.display.float_format = '{:.4f}'.format
    
    print(ex_df1)
    return ex_df1, ex_df2

def saveDf(df):
    exchange = df['exchange'][0]
    symbol = df['symbol'][0].replace('/','')
    df.to_csv('C:\\Users\\Public\\Documents\\%s_%s.csv'%(exchange,symbol),index=False,header=True)
    
    
def plotDf(ex_df1,maWindow):
    maWindow = maWindow*60
    
    ex_df1['ma'] = ex_df1['gimp'].rolling(window=maWindow, min_periods=1).mean()
    ex_df1['spread'] = ex_df1['gimp']-ex_df1['ma']
    
    pd.options.display.float_format = '{:.4f}'.format
    
    print(ex_df1)
    
    # ax1 = plt.subplot(2,1,1)
    # ax2 = plt.subplot(2,1,2)
    
    # ax1.plot(ex_df1['timestamp'],ex_df1['gimp'])
    # ax1.plot(ex_df1['timestamp'],ex_df1['ma'])
    
    # ax2.plot(ex_df1['timestamp'],ex_df1['spread'])
    
    # ax2.axhline(y=0, color = 'r', linewidth=1)
    
    plt.plot(ex_df1['timestamp'],ex_df1['gimp'])
    plt.plot(ex_df1['timestamp'],ex_df1['ma'])
    
    print(ex_df1['gimp'].min())

    #plt.fill_between(ex_df1['timestamp'], ex_df1['gimp'], 0, where=ex_df1['spread']>=0.55, facecolor='red', alpha=0.5)
    plt.fill_between(ex_df1['timestamp'], 0.0, ex_df1['gimp'].max(), where=(abs(ex_df1['spread'])>=0.5), facecolor='red', alpha=0.5)
    
    plt.show()
    
    
    
    
    
    
    
    
    
    
    