import requests
import pyarrow.csv as pacsv
import ccxt
import datetime
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 



# 저장하는 부분과 ohlcv를 따로 결과를 가져오는것을 분리했으면 좋겠습니다.
# 예를들어 ohlcv_binance df를 따로 가져오고, save_ohlcv function을 따로 만드는게 좋을거 같습니다.
# 환율 데이터는 https://github.com/FinanceData/FinanceDataReader/
# 이 라이브러리를 쓰면 좋을것 같아요.
# 제생각에는 binance_ohlcv dataframe을 따로 결과값을 내고, 위의 라이브러리를 이용해서 환율 dataframe을 가져온후
# day에 맞춰서 조인을 한 후 환율을 고려한 dataframe을 만드는 게 더 깔끔하지 않을까 싶습니다.
def ohlcv_binance(symbol, start, end):

    binance = ccxt.binance({'options' : { 'defaultType': 'future'}}) 
                
    start=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    usdkrw_date = start
    start=start-datetime.timedelta(hours=9)   # utc 시간

    end = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    end = end-datetime.timedelta(hours=9) 
        
    start1=start.strftime("%Y-%m-%d %H:%M:%S")
    since1 = binance.parse8601(start1)
        
    timestamp=[]

    price=[]


    while start<end:
                        
        ohlcvs = binance.fetch_ohlcv(symbol , timeframe='1m', since=since1, limit=1000)
        
        for i in ohlcvs:
            timestamp.append(i[0])
            price.append(i[4])
            
        start = start + datetime.timedelta(minutes=1000)
        # 1000으로 설정한 이유는 뭔가요? binance는 한번에 500개씩 가져올수 있는거 같은데 마지막 봉의 timeframe
        # 으로부터 60000 milliseconds 부터 가져오는 식으로 바꾸는게 좋을 것 같습니다.
        start1=start.strftime("%Y-%m-%d %H:%M:%S")
        since1 = binance.parse8601(start1)

        time.sleep(1)
        
    binance = pd.DataFrame({'timestamp' : timestamp, 'price' : price})
    binance = binance.drop_duplicates(['timestamp'], keep = 'first', ignore_index = True)
    binance['timestamp']=pd.to_datetime(binance['timestamp']/1000, unit='s')
    binance['timestamp']=binance['timestamp']+datetime.timedelta(hours=9)
    
    # 여기서부터 두부분으로 나누면 좋을 것 같아요.
    file1 = 'C:\\Users\\홍사\\Documents\\USD_KRW 내역.csv'
    #file2 = 'C:\\Users\\홍사\\Documents\\binance_future_BTCUSDT.csv'
    #file3 = 'C:\\Users\\홍사\\Documents\\upbit_spot_KRW-BTC.csv'
    
    usdkrw = pacsv.read_csv(file1).to_pandas()
    usdkrw = usdkrw.sort_index(ascending=False)
    
    
    usdkrw = usdkrw.reset_index(drop=True)
    binance = binance.reset_index(drop=True)
    
    for i in range(len(usdkrw)):
       usdkrw['날짜'][i] = datetime.datetime.strptime(usdkrw['날짜'][i], "%Y년 %m월 %d일")
       usdkrw['종가'][i] = usdkrw['종가'][i].replace(',',"")
    
    usdkrw = usdkrw[usdkrw_date<=usdkrw['날짜']]
    usdkrw = usdkrw.reset_index(drop=True)
    print(usdkrw)
    
    binance['usdkrw'] = None
    
    usdkrw_index = 0
    for i in binance.index:
        if binance.at[i,'timestamp'].day == usdkrw.at[usdkrw_index,'날짜'].day:
            binance.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'종가'])
        else :
            if (len(usdkrw)-1) < (usdkrw_index+1) :
                binance.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'종가'])
                
            elif usdkrw.at[usdkrw_index+1,'날짜'].day == binance.at[i,'timestamp'].day :
                usdkrw_index += 1
                binance.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'종가'])
            
            else :
                binance.at[i,'usdkrw'] = float(usdkrw.at[usdkrw_index,'종가'])
    
    binance['krw_price'] = binance['price']*binance['usdkrw']

    pd.options.display.float_format = '{:.2f}'.format
    binance['exchange'] = 'binance'
    
    print(binance)
    
    #df.to_csv('C:\\Users\\seung\\Desktop\\2X_Trade_Start_Set\\binance_future_%s.csv'%(symbol.replace('/','')),index=False,header=True)
    binance.to_csv('C:\\Users\\홍사\\Documents\\binance_future_%s.csv'%(symbol.replace('/','')),index=False,header=True)
    return(binance)
    


# 함수를 거래소별로 나누는것은 별로 좋지 않은것 같습니다. 단순 ohlcv를 가져오는 것을
# 어떤 거래소도 가져올 수 있게 함수를 만들고, 그 이후에 환율 데이터를 받아와서 환율을 고려하는 작업을 하는게 좋을것 같습니다.

def ohlcv_upbit(symbol, start, end):
    start=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    end=datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    
    start = start -datetime.timedelta(hours=9)+ datetime.timedelta(minutes=200)
    symbol = symbol
    
    timestamp = []
    price = []
    
    
    while start<end :
    
        url = "https://api.upbit.com/v1/candles/minutes/1?market=%s&to=%s&count=200"%(symbol,start)
        #headers = {"Accept": "application/json"}
        
        response = requests.get(url).json()
        
        for i in response:
            price.append(i['trade_price'])
            timestamp.append(i['candle_date_time_kst'])
        
        start = start + datetime.timedelta(minutes=200)
        time.sleep(0.05)
        
    df = pd.DataFrame({'timestamp' : timestamp, 'price' : price})
    df = df.sort_values('timestamp',ascending=True,ignore_index=True)
    df = df.drop_duplicates(['timestamp'], keep='first', ignore_index=True)
    df['timestamp']=pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M:%S")
    df['exchange'] = 'upbit'
    print(df)
    
    df.to_csv('C:\\Users\\홍사\\Documents\\upbit_spot_%s.csv'%(symbol.replace('/','')),index=False,header=True)
    return df

# main.py 로 옮기는게 좋을것 같습니다.

## 환율 데이터는 따로 다운받아야함.
# binance = ohlcv_binance('BTC/USDT','2022-01-25 00:00:00', noww)         #argument (symbol, since, to)
# upbit = ohlcv_upbit('KRW-BTC','2022-01-25 00:00:00', noww)

# ## binance와 upbit timestamp 동기화 작업.
# print('start data parsing')
# df = pd.concat([binance,upbit])
# df.sort_values('timestamp', ascending=True)
# df.drop_duplicates(['timestamp'], keep=False, inplace = True)

# print(df)

# for i in df.index:
#     if df['exchange'][i] == 'binance' :
#         binance.drop([i],inplace=True)

#     else:
#         upbit.drop([i], inplace=True)

# binance.reset_index(drop=True, inplace=True)
# upbit.reset_index(drop=True, inplace=True)

# print(binance)
# print(upbit)

# ## plot 그리기
# upbit['gimp'] = (upbit['price']-binance['krw_price'])/binance['krw_price'] * 100
# upbit['ma'] = upbit['gimp'].rolling(window=1440, min_periods=1).mean()

# upbit.plot(x='timestamp',y=['gimp','ma'])

