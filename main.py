import gimp2
import pandas as pd
import time
import datetime
import matplotlib.pyplot as plt

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 

if __name__ == '__main__':
    
    startTime = '2022-02-20 00:00:00'
    startTime_usdkrw = startTime[:10]
    endTime = noww
    
    #ex_1은 외국거래소 , ex_2는 한국거래소 
    ex_1 = 'binance'
    ex_2 = 'upbit'
    
    ex_df1 = gimp2.ohlcv(ex_1,'BTC/USDT', startTime, endTime)
    ex_df2 = gimp2.ohlcv(ex_2,'BTC/KRW', startTime, endTime)
    
    ex_df1, ex_df2 = gimp2.dfParsing(ex_df1, ex_df2, startTime_usdkrw)
    
    #gimp2.saveDf(ex_df1)
    #gimp2.saveDf(ex_df2)
    
    #maWindow는 시간단위 
    gimp2.plotDf(ex_df1,maWindow=24)