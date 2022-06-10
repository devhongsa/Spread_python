import datetime
import pandas as pd
import config
import numpy as np
from multiprocessing import Pool


def orderbook(date_from, date_to, ex_param, orderbookSize):
        
    date_from = date_from
    date_to = date_to
    
    ex_1 = ex_param['spot']
    symbol_1 = ex_param['spot_symbol']

    ex_2 = ex_param['future']
    symbol_2 = ex_param['future_symbol']
    
    pool = Pool(2)
    
    result = pool.starmap(orderbook2,[[ex_1, symbol_1,date_from, date_to, orderbookSize],[ex_2, symbol_2, date_from, date_to, orderbookSize]])
    
    pool.close()
    pool.join()
    
    #spot = gimp.orderbook(ex_1, symbol_1,date_from, date_to)
    #future = gimp.orderbook(ex_2, symbol_2, date_from, date_to)

    return result[0], result[1]   
    #return spot, future

def orderbook2(exchange, symbol, date_from, date_to, orderbookSize):
    
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
                "s3://bx-landing/tardis/book_snapshot_{}/{}/{}/{}/{}/{}/data.csv.gz".format(orderbookSize,exchange,symbol,date[0],date[1],date[2]),
                 storage_options={
                    "key": config.AWS_KEY,
                    "secret": config.AWS_SECRET
                },
            )
            
            #df = df.loc[:,['exchange','local_timestamp','asks[0].price','asks[0].amount','bids[0].price','bids[0].amount']]
            
            df['local_timestamp']=pd.to_datetime(df['local_timestamp']/1000000, unit='s')
            df['local_timestamp']=pd.to_datetime(df['local_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'))
            
            df = df.groupby('local_timestamp').tail(1)
            
        
            ex_df = pd.concat([ex_df,df])
            
            print(exchange, date_from_str)
            date_from = date_from + datetime.timedelta(days=1)
            
    except Exception as ex:
        print('에러가 발생했습니다.', ex)
    
    #ex_df.rename(columns={'local_timestamp':'timestamp','asks[0].price':'askPrice','asks[0].amount':'askAmount','bids[0].price':'bidPrice',
                          #'bids[0].amount':'bidAmount'}, inplace=True)
    ex_df = ex_df.reset_index(drop=True)
    ex_df['symbol'] = symbol
    ex_df.drop(columns=['timestamp'])
    print('finish load {} orderbook'.format(exchange))    
    return ex_df


def dfParsing(ex_df1, ex_df2):
    #2개 거래소 timestamp 동기화
    ex_1 = ex_df1.at[0,'exchange']
    ex_2 = ex_df2.at[0,'exchange']
    
    print('start timestamp-data parsing')
    df = pd.concat([ex_df1,ex_df2])
    
    df.sort_values('local_timestamp', ascending=True, inplace = True)
  
    df.drop_duplicates(['local_timestamp'], keep=False, inplace = True)
    
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
    
    ex_df1['buySpread'] = (ex_df2['asks[0].price']-ex_df1['bids[0].price'])/ex_df1['bids[0].price']*100
    ex_df1['sellSpread'] = (ex_df2['bids[0].price']-ex_df1['asks[0].price'])/ex_df1['asks[0].price']*100
    
    return ex_df1, ex_df2 


def slippage(ex_df1, ex_df2, asset, spreadFrom, orderbookSize):
    
    df = pd.DataFrame(columns=['buySpread','sellSpread', 'bestSpotPrice', 'spotAvgPrice', 'bestFuturePrice', 'futreAvgPrice', 'spotSlippage', 'futureSlippage', 'outOfRange_S','outOfRange_F'])
    
    
    for i in ex_df1.index:
        
        if ex_df1.at[i,'buySpread'] <= -spreadFrom:
            spotPrice = []
            spotAmount = []
            spotAsset = asset 
            
            futurePrice = []
            futureAmount = []
            futureAsset = asset 
            
            buySpread = ex_df1.at[i,'buySpread']
            bestSpotPrice = ex_df1.at[i,'bids[0].price']
            bestFuturePrice = ex_df2.at[i,'asks[0].price']
            
            outOfRange_S = False
            outOfRange_F = False
            
            for j in range(orderbookSize):
                price2 = ex_df1.at[i, f'bids[{j}].price']
                amount = ex_df1.at[i, f'bids[{j}].amount']
                Spare = spotAsset - price2*amount
                if Spare > 0:
                    spotPrice.append(price2)
                    spotAmount.append(amount)
                    spotAsset = Spare
                    
                else :
                    spotPrice.append(price2)
                    spotAmount.append(spotAsset/price2)
                    break
                if j == orderbookSize-1:
                    outOfRange_S = True
                    
            for j in range(orderbookSize):
                price2 = ex_df2.at[i, f'asks[{j}].price']
                amount = ex_df2.at[i, f'asks[{j}].amount']
                Spare = futureAsset - price2*amount
                if Spare > 0:
                    futurePrice.append(price2)
                    futureAmount.append(amount)
                    futureAsset = Spare
                    
                else :
                    futurePrice.append(price2)
                    futureAmount.append(futureAsset/price2)
                    break
                if j == orderbookSize-1:
                    outOfRange_F = True
                    
            spotAvgPrice = (np.array(spotPrice)*np.array(spotAmount)).sum()/np.array(spotAmount).sum()
            futureAvgPrice = (np.array(futurePrice)*np.array(futureAmount)).sum()/np.array(futureAmount).sum()
            df.loc[len(df)] = [buySpread,0, bestSpotPrice, spotAvgPrice, bestFuturePrice, futureAvgPrice, (bestSpotPrice-spotAvgPrice)/bestSpotPrice*100, (futureAvgPrice-bestFuturePrice)/bestFuturePrice*100, outOfRange_S, outOfRange_F]
            
        if ex_df1.at[i,'sellSpread'] >= spreadFrom:
            spotPrice = []
            spotAmount = []
            spotAsset = asset 
            
            futurePrice = []
            futureAmount = []
            futureAsset = asset 
            
            sellSpread = ex_df1.at[i,'sellSpread']
            bestSpotPrice = ex_df1.at[i,'asks[0].price']
            bestFuturePrice = ex_df2.at[i,'bids[0].price']

            outOfRange_S = False
            outOfRange_F = False

            for j in range(orderbookSize):
                price2 = ex_df1.at[i, f'asks[{j}].price']
                amount = ex_df1.at[i, f'asks[{j}].amount']
                Spare = spotAsset - price2*amount
                if Spare > 0:
                    spotPrice.append(price2)
                    spotAmount.append(amount)
                    spotAsset = Spare
                    
                else :
                    spotPrice.append(price2)
                    spotAmount.append(spotAsset/price2)
                    break
                if j == orderbookSize-1:
                    outOfRange_S = True
                    
            for j in range(orderbookSize):
                price2 = ex_df2.at[i, f'bids[{j}].price']
                amount = ex_df2.at[i, f'bids[{j}].amount']
                Spare = futureAsset - price2*amount
                if Spare > 0:
                    futurePrice.append(price2)
                    futureAmount.append(amount)
                    futureAsset = Spare
                    
                else :
                    futurePrice.append(price2)
                    futureAmount.append(futureAsset/price2)
                    break
                if j == orderbookSize-1:
                    outOfRange_F = True
                    
            spotAvgPrice = (np.array(spotPrice)*np.array(spotAmount)).sum()/np.array(spotAmount).sum()
            futureAvgPrice = (np.array(futurePrice)*np.array(futureAmount)).sum()/np.array(futureAmount).sum()
            df.loc[len(df)] = [0,sellSpread, bestSpotPrice, spotAvgPrice, bestFuturePrice, futureAvgPrice, (spotAvgPrice-bestSpotPrice)/bestSpotPrice*100, (bestFuturePrice-futureAvgPrice)/bestFuturePrice*100, outOfRange_S, outOfRange_F]
    
    return df

if __name__ == "__main__":
    
    #spot : binance(BTCUSDT)0.00075, huobi(BTCUSDT)0.002, okex(BTC-USDT)0.001
    #future : binance-futures(BTCUSDT)0.0004, bitmex, bybit, ftx, huobi-dm-swap(BTC-USD)0.0004, okex-swap(BTC-USDT-SWAP)0.0005
    
    
    ex_param = {'spot' : 'binance',             #spot
                'future' : 'binance-futures',       #future
                'spot_symbol': 'BTCUSDT',
                'future_symbol': 'BTCUSDT'}
    
    date_from = '2022-03-01'
    date_to = '2022-03-30'
    
    orderbookSize = 25      # 5 or 25
    
    
    ex_df1, ex_df2 = orderbook(date_from, date_to, ex_param, orderbookSize)
    ex_df1, ex_df2 = dfParsing(ex_df1, ex_df2)
    
    slippageDf = slippage(ex_df1, ex_df2, 100000, 0.22, orderbookSize)
    
    slippageDf.to_csv('./slippage.csv')
    