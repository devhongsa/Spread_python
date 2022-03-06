import gimp2
import pandas as pd
import time
import datetime

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 

if __name__ == '__main__':
    
    startTime = '2022-03-04 00:00:00'
    startTime_usdkrw = startTime[:10]
    endTime = noww
    
    #ex_1은 외국거래소 , ex_2는 한국거래소 
    ex_1 = 'binance'
    ex_2 = 'upbit'
    
    ex_df1 = gimp2.ohlcv(ex_1,'BTC/USDT', startTime, endTime)
    ex_df2 = gimp2.ohlcv(ex_2,'BTC/KRW', startTime, endTime)
    
    ex_df1, ex_df2 = gimp2.dfParsing(ex_df1, ex_df2, startTime_usdkrw)
    

    ex_df1['ma'] = ex_df1['gimp'].rolling(window=1440, min_periods=1).mean()

    ex_df1.plot(x='timestamp',y=['gimp','ma'])

