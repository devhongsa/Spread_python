import FinanceDataReader as fdr
import requests
import ccxt
import datetime
import time
import pandas as pd
import exchangeOption as op
import matplotlib.pyplot as plt
import mlflow
from mlflow import log_metric, log_param, log_artifacts
import config

def ohlcv(exchange, symbol, start, end):
    Exchange = op.exchangeOption(exchange)
    
    start=datetime.datetime.strptime(start,'%Y-%m-%d')
    #start=start-datetime.timedelta(hours=9)   # utc 시간
    
    end = '{} 23:59:59'.format(end)
    end = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    #end = end-datetime.timedelta(hours=9)     #
    
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
        
    
    ex_df = pd.DataFrame({'timestamp' : timestamp, 'bidPrice' : price, 'askPrice': price})
    ex_df['timestamp']=pd.to_datetime(ex_df['timestamp']/1000, unit='s')
    #ex_df['timestamp']=ex_df['timestamp']+datetime.timedelta(hours=9)    #
    ex_df['exchange'] = exchange
    ex_df['symbol'] = symbol
    print(ex_df['exchange'][0] + ' done')
    return ex_df
    


def orderbook(exchange, symbol, date_from, date_to):
    
    try:
        ex_df = pd.DataFrame()
        date_from = date_from[:10]
        date_to = date_to[:10]
        date_from=datetime.datetime.strptime(date_from,'%Y-%m-%d')
        date_to=datetime.datetime.strptime(date_to,'%Y-%m-%d')
        
        while date_from <= date_to:
            
            date_from_str = datetime.datetime.strftime(date_from,'%Y-%m-%d')
            date = date_from_str.split('-')
            
            df = pd.read_csv(
                "s3://bx-landing/tardis/book_snapshot_5/{}/{}/{}/{}/{}/data.csv.gz".format(exchange,symbol,date[0],date[1],date[2]),
                 storage_options={
                    "key": config.AWS_KEY,
                    "secret": config.AWS_SECRET
                },
            )
            
            df = df.loc[:,['exchange','local_timestamp','asks[0].price','asks[0].amount','bids[0].price','bids[0].amount']]
            
            df['local_timestamp']=pd.to_datetime(df['local_timestamp']/1000000, unit='s')
            df['local_timestamp']=pd.to_datetime(df['local_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'))
            
            df = df.groupby('local_timestamp').tail(1)
            
        
            ex_df = pd.concat([ex_df,df])
            
            print(exchange, date_from_str)
            date_from = date_from + datetime.timedelta(days=1)

                        

    except Exception as ex:
        print('에러가 발생했습니다.', ex)
    
    ex_df.rename(columns={'local_timestamp':'timestamp','asks[0].price':'askPrice','asks[0].amount':'askAmount','bids[0].price':'bidPrice',
                          'bids[0].amount':'bidAmount'}, inplace=True)
    ex_df = ex_df.reset_index(drop=True)
    ex_df['symbol'] = symbol
    print('finish load {} orderbook'.format(exchange))    
    return ex_df

    
def dfParsing(ex_df1, ex_df2, start, maWindow, data):
    
    #2개 거래소 timestamp 동기화
    ex_1 = ex_df1.at[0,'exchange']
    ex_2 = ex_df2.at[0,'exchange']
    
    print('start timestamp-data parsing')
    df = pd.concat([ex_df1,ex_df2])
    
    df.sort_values('timestamp', ascending=True, inplace = True)
  
    df.drop_duplicates(['timestamp'], keep=False, inplace = True)
    
    ex1_index = []
    ex2_index = []
    
    lenDf = len(df)

    if lenDf>0:
        for i in df.index:
            
            if type(df.at[i,'exchange']) == str: 
                if df.at[i,'exchange'] == ex_1:
                    ex1_index.append(i)
                    print(ex_1, i, df.index[-1])
                else:
                    ex2_index.append(i)
                    print(ex_2 ,i, df.index[-1])
            else:
                ex1_index.append(i)
                ex2_index.append(i)
                
    ex1_index = list(set(ex1_index))
    ex2_index = list(set(ex2_index))
    
    ex_df1.drop(ex1_index, inplace=True)
    ex_df2.drop(ex2_index, inplace=True)

    ex_df1.reset_index(drop=True, inplace=True)
    ex_df2.reset_index(drop=True, inplace=True)

   
    
    #환율데이터 삽입 
    #################### finance-datareader 오류시 사용 ##########################
    # start_str = start
    # date = datetime.datetime.strptime(start_str,'%Y-%m-%d')
    
    # today = datetime.datetime.now() - datetime.timedelta(days=1)
    
    # timestamp = []
    # usd_krw = []

    # while date<today:
            
    #     url = 'https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/%s/currencies/krw/usd.min.json'%start_str
    #     response = requests.get(url).json()
    #     timestamp.append(response['date'])
    #     usd_krw.append(1/response['usd'])    
        
    #     date = date + datetime.timedelta(days=1)
    #     start_str = date.strftime("%Y-%m-%d")
        
    #     time.sleep(0.05)
        
    # usdkrw = pd.DataFrame({'Date':timestamp,'Close':usd_krw})
    # usdkrw['Date']=pd.to_datetime(usdkrw['Date'])
    #############################################################################
    
    print("start ex_rate data parsing")
    usdkrw = fdr.DataReader('USD/KRW', start)
    usdkrw = usdkrw.reset_index(drop=False)
    
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
    
    ex_df1['krw_bidPrice'] = ex_df1['bidPrice']*ex_df1['usdkrw']
    ex_df1['krw_askPrice'] = ex_df1['askPrice']*ex_df1['usdkrw']
    
    
    
    #Gimp 데이터 추가 
    print("start Gimp data parsing")
    ex_df1['sellGimp'] = (ex_df2['bidPrice']-ex_df1['krw_askPrice'])/ex_df1['krw_askPrice'] * 100
    ex_df1['buyGimp'] = (ex_df2['askPrice']-ex_df1['krw_bidPrice'])/ex_df1['krw_bidPrice'] * 100
    #ma , spread 데이터 추가 
    
    if data == '1s':   
        maWindow = maWindow*60*60
    elif data == '1m':
        maWindow = maWindow*60
    
    ex_df1['buy_ma'] = ex_df1['buyGimp'].rolling(window=maWindow, min_periods=1).mean()
    ex_df1['buySpread'] = ex_df1['buyGimp']-ex_df1['buy_ma']
    
    ex_df1['sell_ma'] = ex_df1['sellGimp'].rolling(window=maWindow, min_periods=1).mean()
    ex_df1['sellSpread'] = ex_df1['sellGimp']-ex_df1['sell_ma']
    
    pd.options.display.float_format = '{:.4f}'.format
    
    #ex_df1.to_csv('./Dataframe/{}_{}.xlsx'.format(ex_1,start), mode='w')
    #ex_df2.to_csv('./Dataframe/{}_{}.xlsx'.format(ex_2,start), mode='w')
    
    return ex_df1, ex_df2

def saveDf(df):
    exchange = df.at[0,'exchange']
    symbol = df['symbol'][0].replace('/','')
    df.to_csv('C:\\Users\\Public\\Documents\\%s_%s.csv'%(exchange,symbol),index=False,header=True)
    
    
def plotDf(ex_df1, buyIndex, sellIndex):
    
    #print(ex_df1)
    
    # ax1 = plt.subplot(2,1,1)
    # ax2 = plt.subplot(2,1,2)
    
    # ax1.plot(ex_df1['timestamp'],ex_df1['gimp'])
    # ax1.plot(ex_df1['timestamp'],ex_df1['ma'])
    
    # ax2.plot(ex_df1['timestamp'],ex_df1['spread'])
    
    # ax2.axhline(y=0, color = 'r', linewidth=1)
    
    plt.plot(ex_df1['timestamp'],ex_df1['buyGimp'])
    plt.plot(ex_df1['timestamp'],ex_df1['buy_ma'])

    plt.fill_between(ex_df1['timestamp'], ex_df1['buyGimp'].min(), ex_df1['buyGimp'].max(), where=(abs(ex_df1['buySpread'])>=0.6), facecolor='red', alpha=0.5)
    
    for i in buyIndex:
        plt.annotate('buy',xy=(ex_df1['timestamp'][i],ex_df1['buyGimp'][i]), xytext=(ex_df1['timestamp'][i],ex_df1['buyGimp'][i]+0.1),arrowprops=dict(facecolor='red'))
    for i in sellIndex:
        plt.annotate('sell',xy=(ex_df1['timestamp'][i],ex_df1['sellGimp'][i]), xytext=(ex_df1['timestamp'][i],ex_df1['sellGimp'][i]+0.1),arrowprops=dict(facecolor='blue'))
    
    plt.show()
    
    
def tradingResult(ex_df1, ex_df2, param):
    
    startKrw = param['startAssetKrw']
    startUsd = param['startAssetUsd']
    count = param['count']
    spreadIn = param['spreadInFrom']
    spreadAddIn = param['spreadAddIn']
    spreadOut = param['spreadOutFrom']
    slippage = param['slippage']
    
    dfBuy = pd.DataFrame(columns=['timestamp','buyInPrice','sellInPrice','buySpread','amount','gimp'])
    dfSell = pd.DataFrame(columns=['timestamp','buyOutPrice','sellOutPrice','sellSpread','amount','gimp','usdkrw'])
    
    outControl = False
    
    buyIndex = []
    sellIndex = []
    
    countReset = count
    #진입청산 조건에 따른 매매시점 
    for i in ex_df1.index:
        
        if outControl == False : 
            if ex_df1.at[i,'buySpread'] <= spreadIn:
                dfBuy.loc[len(dfBuy)] = [ex_df1.at[i,'timestamp'],ex_df2.at[i,'askPrice'], ex_df1.at[i,'bidPrice'],ex_df1.at[i,'buySpread'],
                                         (startKrw/count)/ex_df2.at[i,'askPrice'], ex_df1.at[i,'buyGimp']]
                buyIndex.append(i)
                outControl = True
                countReset -= 1
                print('buy')
        
        elif outControl == True and ex_df1.at[i,'buySpread'] - dfBuy['buySpread'].iloc[-1] <= spreadAddIn and countReset != 0:
            dfBuy.loc[len(dfBuy)] = [ex_df1.at[i,'timestamp'],ex_df2.at[i,'askPrice'], ex_df1.at[i,'bidPrice'],ex_df1.at[i,'buySpread'],
                                         (startKrw/count)/ex_df2.at[i,'askPrice'], ex_df1.at[i,'buyGimp']]
            buyIndex.append(i)
            countReset -= 1
            print('additional buying')
            
            
        elif outControl == True and ex_df1.at[i,'sellSpread'] >= spreadOut:
            dfSell.loc[len(dfSell)] = [ex_df1.at[i,'timestamp'],ex_df2.at[i,'bidPrice'], ex_df1.at[i,'askPrice'],ex_df1.at[i,'sellSpread'],
                                         1, ex_df1.at[i,'sellGimp'], ex_df1.at[i,'usdkrw']]
            sellIndex.append(i)
            outControl = False
            countReset = count
            print('sell')
    
    df = pd.concat([dfBuy,dfSell])
    df.sort_values('timestamp', ascending = True, inplace = True)
    df.reset_index(drop=True,inplace = True)

    #slippage 설정
    slippage = slippage/100
    df['buyInPrice'] *= (1+slippage)
    df['sellInPrice'] *= (1-slippage)
    df['buyOutPrice'] *= (1 - slippage)
    df['sellOutPrice'] *= (1 + slippage)
    
    resultDf = pd.DataFrame(columns=['avgPriceKrw','outPriceKrw','avgPriceUsd','outPriceUsd','avgGimp','outGimp','avgSpread','outSpread','totalAmt','profitKrw','profitUsd','usdkrw'])
    startIndex = 0

    #손익 분석 
    for i in df.index:
        if pd.isna(df.at[i,'buyInPrice']):
            totalAmt = df['amount'].iloc[startIndex:i].sum()
            avgPriceKrw = (df['buyInPrice'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            avgPriceUsd = (df['sellInPrice'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            avgGimp = (df['gimp'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            avgSpread = (df['buySpread'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            
            outSpread = df.at[i,'sellSpread']
            outGimp = df.at[i,'gimp']
            outPriceKrw = df.at[i,'buyOutPrice']
            outPriceUsd = df.at[i,'sellOutPrice']
            FeeKrw = avgPriceKrw*totalAmt*0.0005 + outPriceKrw*totalAmt*0.0005
            FeeUsd = avgPriceUsd*totalAmt*0.00075 + outPriceUsd*totalAmt*0.00075
           
            profitKrw = (outPriceKrw - avgPriceKrw)*totalAmt - FeeKrw
            profitUsd = (avgPriceUsd - outPriceUsd)*totalAmt - FeeUsd
            
            usdkrw = df.at[i,'usdkrw']
            
            resultDf.loc[len(resultDf)] = [avgPriceKrw, outPriceKrw, avgPriceUsd, outPriceUsd, avgGimp, outGimp, avgSpread, outSpread, totalAmt, profitKrw, profitUsd, usdkrw]

            startIndex = i+1
    
    pd.options.display.float_format = '{:.4f}'.format
    
    pnlDf = pd.DataFrame() 
    
    pnlDf['초기자산'] = startKrw + startUsd*resultDf['usdkrw']*(1+resultDf['outGimp']/100)
    pnlDf['손익KRW환산'] = resultDf['profitUsd']*resultDf['usdkrw']*(1+resultDf['outGimp']/100) + resultDf['profitKrw']
    pnlDf['현재자산'] = 0
    
    for i in pnlDf.index:    
        pnlDf.at[i,'현재자산'] = pnlDf.at[i,'초기자산'] + pnlDf['손익KRW환산'].iloc[:i+1].sum()
    pnlDf['수익률'] = pnlDf['손익KRW환산']/pnlDf['초기자산']*100
    pnlDf['총수익률'] = (pnlDf['현재자산']-pnlDf['초기자산'])/pnlDf['초기자산']*100
    
    if len(pnlDf) == 0:
        pnlDf.loc[0] = [0,0,0,0,0]
    
    profitCount = len(pnlDf[0<pnlDf['수익률']])
    
    #print(resultDf)
    print(pnlDf)
    print('\n')
    print("spreadIn : {}".format(spreadIn))
    print("spreadOut: {}".format(spreadOut))
    print('매매 횟수 : {}'.format(len(pnlDf)))
    if len(pnlDf) != 0:
        print('수익 횟수 : {} 수익 확률 : {}%'.format(profitCount,round(profitCount/len(pnlDf)*100,2)))
        print('매매당 수익률 평균 : {}%'.format(round(pnlDf['수익률'].mean(),4)))
        print('총수익률 : {}%'.format(round(pnlDf['총수익률'].iloc[-1],4)))
        
    return pnlDf, buyIndex, sellIndex
    

def mf(ex_df1, ex_df2, param):
    
    startAssetKrw = param['startAssetKrw']
    startAssetUsd = param['startAssetUsd']

    mlflow.set_tracking_uri('http://127.0.0.1:5000')
    mlflow.set_experiment('Test_3')

    spreadInFrom = param['spreadInFrom']
    spreadInTo = param['spreadInTo']
    spreadAddIn = param['spreadAddIn']

    spreadInDelta = param['spreadInDelta']
    spreadOutDelta = param['spreadOutDelta']
    
    result = pd.DataFrame(columns=['spreadIn', 'spreadOut', 'profitRate', 'tradingCount', 'profit_Probability'])


    with mlflow.start_run():
        
        while round(spreadInFrom,2) <= spreadInTo:
            outSpread = param['spreadOutFrom']
            until = param['spreadOutTo']
            while round(outSpread,2) <= until:
            
                with mlflow.start_run(nested=True):
                    pnlDf, buyIndex, sellIndex = tradingResult(ex_df1, ex_df2, 
                    {'startAssetKrw' : startAssetKrw,
                     'startAssetUsd' : startAssetUsd,
                     'spreadInFrom' : spreadInFrom,
                     'spreadAddIn' : spreadAddIn ,
                     'spreadOutFrom' : outSpread,
                     'slippage': param['slippage'],
                     'count' : param['count'],
                     'mlflow': param['mlflow']
                     })
                    param_mlflow = {'spreadIn':spreadInFrom, 'spreadOut':outSpread}
                    mlflow.log_params(param_mlflow)
                    
                    profitRate = round(pnlDf['총수익률'].iloc[-1],4)
                    tradingCount = len(pnlDf)
                    profit_prob = round(len(pnlDf[0<pnlDf['수익률']])/tradingCount*100,2)
                    
                    result.loc[len(result)] = [spreadInFrom, outSpread, profitRate, tradingCount, profit_prob]

                    log_metric("profitRate", profitRate)
                    log_metric("tradingCount", tradingCount)
                    log_metric('profit_probability', profit_prob)
                    outSpread += spreadOutDelta
                    outSpread = round(outSpread,2)
                    
            spreadInFrom += spreadInDelta
            spreadInFrom = round(spreadInFrom,2)
    
    return result, buyIndex, sellIndex
    
    