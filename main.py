import gimp2
import numpy as np
import pandas as pd
import time
import datetime

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 

if __name__ == '__main__':
    
    startTime = '2022-03-01 00:00:00'
    startTime_usdkrw = startTime[:10]
    endTime = noww
    
    #ex_1은 외국거래소 , ex_2는 한국거래소 
    ex_1 = 'binance'
    ex_2 = 'upbit'
    
    #각 거래소 ohlcv 리턴
    ex_df1 = gimp2.ohlcv(ex_1,'BTC/USDT', startTime, endTime)
    ex_df2 = gimp2.ohlcv(ex_2,'BTC/KRW', startTime, endTime)
    
    
    #2개 거래소 timestamp 동기화 및 여러 data  dataframe 추가 
    ex_df1, ex_df2 = gimp2.dfParsing(ex_df1, ex_df2, startTime_usdkrw, maWindow=24)  #maWindow는 시간단위 
    
    #csv파일로 저장 
    #gimp2.saveDf(ex_df1)
    #gimp2.saveDf(ex_df2)

    #최종 dataframe
    print(ex_df1)
    print(ex_df2)
    
    #매매기록, 손익분석 dataframe 리턴
    #buyIndex sellIndex는 매매했던 시점의 dataframe 인덱스 넘버.  이 인덱스넘버는 plot에서 매매시점 표시할때 사용.
    resultDf, buyIndex, sellIndex = gimp2.tradingResult(ex_df1, ex_df2, spreadIn=-0.6, spreadAddIn = -0.1, spreadOut = -0.1, amount = 1)
    
    #plot 그리기 
    gimp2.plotDf(ex_df1, buyIndex, sellIndex)