import gimp
import pandas as pd
import time
import datetime

now=time.time()   #unixtime
now2=int(now*1000)

now_date=datetime.datetime.fromtimestamp(now)  #unixtime to datetime

noww=now_date.strftime("%Y-%m-%d %H:%M:%S")  #datetime to string,  이거는 end argument에 넣을 꺼 

if __name__ == '__main__':
    
    binance = gimp.ohlcv_binance('BTC/USDT','2022-01-25 00:00:00', noww)         #argument (symbol, since, to)
    upbit = gimp.ohlcv_upbit('KRW-BTC','2022-01-25 00:00:00', noww)

    ## binance와 upbit timestamp 동기화 작업.
    print('start data parsing')
    df = pd.concat([binance,upbit])
    df.sort_values('timestamp', ascending=True)
    df.drop_duplicates(['timestamp'], keep=False, inplace = True)

    print(df)

    for i in df.index:
        if df['exchange'][i] == 'binance' :
            binance.drop([i],inplace=True)

        else:
            upbit.drop([i], inplace=True)

    binance.reset_index(drop=True, inplace=True)
    upbit.reset_index(drop=True, inplace=True)

    print(binance)
    print(upbit)

    ## plot 그리기
    upbit['gimp'] = (upbit['price']-binance['krw_price'])/binance['krw_price'] * 100
    upbit['ma'] = upbit['gimp'].rolling(window=1440, min_periods=1).mean()

    upbit.plot(x='timestamp',y=['gimp','ma'])

