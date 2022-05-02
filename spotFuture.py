import pandas as pd
import gimp
import matplotlib.pyplot as plt
import mlflow

def orderbook(date_from, date_to, ex_param):
        
    date_from = date_from
    date_to = date_to
    
    #외국거래소
    ex_1 = ex_param['spot']
    symbol_1 = ex_param['spot_symbol']
        
    #한국거래소 
    ex_2 = ex_param['future']
    symbol_2 = ex_param['future_symbol']
    
    spot = gimp.orderbook(ex_1, symbol_1,date_from, date_to)
    future = gimp.orderbook(ex_2, symbol_2, date_from, date_to)

    return spot, future    
    
    
def dfParsing(ex_df1, ex_df2):
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
    
    ex_df1['buySpread'] = (ex_df1['askPrice']-ex_df2['bidPrice'])/ex_df1['askPrice']*100
    ex_df1['sellSpread'] = (ex_df1['bidPrice']-ex_df2['askPrice'])/ex_df1['bidPrice']*100
    
    return ex_df1, ex_df2 

def plotDf(ex_df1, buyIndex=[], sellIndex=[]):
    
    ex_df1.loc[ex_df1['buySpread']>=0,'buySpread'] = 0
    ex_df1.loc[ex_df1['sellSpread']<=0,'sellSpread'] = 0
    
    fig, ax1 = plt.subplots()
    ax1.plot(ex_df1['timestamp'], ex_df1['buySpread'], color='red')
    ax1.plot(ex_df1['timestamp'], ex_df1['sellSpread'], color='blue')
    
    ax2 = ax1.twinx()
    ax2.plot(ex_df1['timestamp'], ex_df1['askPrice'], color='black')
    
    
    # plt.plot(ex_df1['timestamp'],ex_df1['buySpread'])
    # plt.plot(ex_df1['timestamp'],ex_df1['sellSpread'])
    # plt.plot(ex_df1['timestamp'],ex_df1['askPrice'])
    
    ax1.axhline(y=0, color='r', linewidth=1)
    ax1.axhline(y=0.25, color='r', linewidth=1)
    ax1.axhline(y=-0.25, color='r', linewidth=1)
    
    # plt.fill_between(ex_df1['timestamp'], ex_df1['buySpread'].min(), ex_df1['buySpread'].max(), where=(ex_df1['buySpread']<=-0.3), facecolor='red', alpha=0.5)
    # plt.fill_between(ex_df1['timestamp'], ex_df1['sellSpread'].min(), ex_df1['sellSpread'].max(), where=(ex_df1['sellSpread']>=0.3), facecolor='blue', alpha=0.5)
    
    # for i in buyIndex:
    #     plt.annotate('buy',xy=(ex_df1['timestamp'][i],ex_df1['buyGimp'][i]), xytext=(ex_df1['timestamp'][i],ex_df1['buyGimp'][i]+0.1),arrowprops=dict(facecolor='red'))
    # for i in sellIndex:
    #     plt.annotate('sell',xy=(ex_df1['timestamp'][i],ex_df1['sellGimp'][i]), xytext=(ex_df1['timestamp'][i],ex_df1['sellGimp'][i]+0.1),arrowprops=dict(facecolor='blue'))
    
    plt.show()
    
    
def tradingResult(ex_df1, ex_df2, param):
    
    startUsd = param['startAssetUsd']
    count = param['count']
    spreadIn = param['spreadInFrom']
    spreadAddIn = param['spreadAddIn']
    spreadOut = param['spreadOutFrom']
    slippage = param['slippage']
    
    dfBuy = pd.DataFrame(columns=['timestamp','buyInPrice','sellInPrice','buySpread','amount'])
    dfSell = pd.DataFrame(columns=['timestamp','buyOutPrice','sellOutPrice','sellSpread','amount'])
    
    outControl = False
    
    buyIndex = []
    sellIndex = []
    
    countReset = count
    #진입청산 조건에 따른 매매시점 
    for i in ex_df1.index:
        
        if outControl == False : 
            if ex_df1.at[i,'buySpread'] <= spreadIn:
                dfBuy.loc[len(dfBuy)] = [ex_df1.at[i,'timestamp'],ex_df1.at[i,'askPrice'], ex_df2.at[i,'bidPrice'],ex_df1.at[i,'buySpread'],
                                         (startUsd/count)/ex_df1.at[i,'askPrice']]
                buyIndex.append(i)
                outControl = True
                countReset -= 1
                print('buy')
        
        elif outControl == True and countReset != 0 and ex_df1.at[i,'buySpread'] - dfBuy['buySpread'].iloc[-1] <= spreadAddIn :
            dfBuy.loc[len(dfBuy)] = [ex_df1.at[i,'timestamp'],ex_df2.at[i,'askPrice'], ex_df1.at[i,'bidPrice'],ex_df1.at[i,'buySpread'],
                                         (startUsd/count)/ex_df1.at[i,'askPrice']]
            buyIndex.append(i)
            countReset -= 1
            print('additional buying')
            
            
        elif outControl == True and ex_df1.at[i,'sellSpread'] >= spreadOut:
            dfSell.loc[len(dfSell)] = [ex_df1.at[i,'timestamp'],ex_df1.at[i,'bidPrice'], ex_df2.at[i,'askPrice'],ex_df1.at[i,'sellSpread'],
                                         1,]
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
    
    resultDf = pd.DataFrame(columns=['avgPriceSpot','outPriceSpot','avgPriceFuture','outPriceFuture','avgSpread','outSpread','totalAmt','profitSpot','profitFuture'])
    startIndex = 0

    #손익 분석 
    for i in df.index:
        if pd.isna(df.at[i,'buyInPrice']):
            totalAmt = df['amount'].iloc[startIndex:i].sum()
            avgPriceSpot = (df['buyInPrice'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            avgPriceFuture = (df['sellInPrice'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            avgSpread = (df['buySpread'].iloc[startIndex:i]*df['amount'].iloc[startIndex:i]).sum()/totalAmt
            
            outSpread = df.at[i,'sellSpread']
            outPriceSpot = df.at[i,'buyOutPrice']
            outPriceFuture = df.at[i,'sellOutPrice']
            spotFee = avgPriceSpot*totalAmt*param['spotFee'] + outPriceSpot*totalAmt*param['spotFee']
            futureFee = avgPriceFuture*totalAmt*param['futureFee'] + outPriceFuture*totalAmt*param['futureFee']
           
            profitSpot = (outPriceSpot - avgPriceSpot)*totalAmt - spotFee
            profitFuture = (avgPriceFuture - outPriceFuture)*totalAmt - futureFee
            
            resultDf.loc[len(resultDf)] = [avgPriceSpot, outPriceSpot, avgPriceFuture, outPriceFuture, avgSpread, outSpread, totalAmt, profitSpot, profitFuture]

            startIndex = i+1
    
    pd.options.display.float_format = '{:.4f}'.format
    
    pnlDf = pd.DataFrame() 
    
    
    pnlDf['손익'] = resultDf['profitFuture'] + resultDf['profitSpot']
    pnlDf['초기자산'] = startUsd*2
    pnlDf['현재자산'] = 0
    
    for i in pnlDf.index:    
        pnlDf.at[i,'현재자산'] = pnlDf.at[i,'초기자산'] + pnlDf['손익'].iloc[:i+1].sum()
    pnlDf['수익률'] = pnlDf['손익']/pnlDf['초기자산']*100
    pnlDf['총수익률'] = (pnlDf['현재자산']-pnlDf['초기자산'])/pnlDf['초기자산']*100
    
    #매매가 없었을 시 오류 방지
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
        
    return resultDf, pnlDf, buyIndex, sellIndex    


def mf(ex_df1, ex_df2, param):
    
    startAssetUsd = param['startAssetUsd']

    mlflow.set_tracking_uri('http://127.0.0.1:5000')
    mlflow.set_experiment('binance_binance')

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
                    resultDf, pnlDf, buyIndex, sellIndex = tradingResult(ex_df1, ex_df2, 
                    {
                     'startAssetUsd' : startAssetUsd,
                     'spreadInFrom' : spreadInFrom,
                     'spreadAddIn' : spreadAddIn ,
                     'spreadOutFrom' : outSpread,
                     'slippage': param['slippage'],
                     'count' : param['count'],
                     'mlflow': param['mlflow'],
                     'spotFee': param['spotFee'],
                     'futureFee':param['futureFee']
                     })
                    param_mlflow = {'spreadIn':spreadInFrom, 'spreadOut':outSpread}
                    mlflow.log_params(param_mlflow)
                    
                    profitRate = round(pnlDf['총수익률'].iloc[-1],4)
                    tradingCount = len(pnlDf)
                    profit_prob = round(len(pnlDf[0<pnlDf['수익률']])/tradingCount*100,2)
                    
                    result.loc[len(result)] = [spreadInFrom, outSpread, profitRate, tradingCount, profit_prob]

                    mlflow.log_metric("profitRate", profitRate)
                    mlflow.log_metric("tradingCount", tradingCount)
                    mlflow.log_metric('profit_probability', profit_prob)
                    outSpread += spreadOutDelta
                    outSpread = round(outSpread,2)
                    
            spreadInFrom += spreadInDelta
            spreadInFrom = round(spreadInFrom,2)
    
    return result, buyIndex, sellIndex

if __name__ == "__main__":
    
    #spot : binance, huobi, okex
    #future : binance-futures, bitmex, bybit, ftx, huobi-dm, okex-futures
    
    
    ex_param = {'spot' : 'binance',             #spot
                'future' : 'binance-futures',       #future
                'spot_symbol': 'BTCUSDT',
                'future_symbol': 'BTCUSDT'}
    
    param_mlflow =  {
                     'startAssetUsd' : 100000,
                     'spreadInFrom' : -0.32,
                     'spreadInTo' : -0.22,
                     'spreadAddIn' : -0.1,
                     'spreadOutFrom' : 0.22,
                     'spreadOutTo' : 0.32,
                     'spreadInDelta' : 0.02,
                     'spreadOutDelta' : 0.02,
                     'slippage': 0.1,
                     'count' : 1,
                     'mlflow' : True,
                     'spotFee' : 0.00075,
                     'futureFee' : 0.0004
                     }
    
    date_from = '2022-02-01'
    date_to = '2022-03-30'
    
    ex_df1, ex_df2 = orderbook(date_from, date_to, ex_param)
    spot, future = dfParsing(ex_df1, ex_df2)
    plotDf(spot)
    #resultDf, pnlDf, buyIndex, sellIndex = tradingResult(spot, future, param_mlflow)
    result, buyIndex, sellIndex = mf(spot,future,param_mlflow)
    