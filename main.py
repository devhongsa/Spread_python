import gimp

def main_aws(date_from, date_to, param, ex_param):
        
    date_from = date_from
    date_to = date_to
    
    #외국거래소
    ex_1 = ex_param['ex_1']
    symbol_1 = '{}USDT'.format(ex_param['symbol'])
    
    #한국거래소 
    ex_2 = ex_param['ex_2']
    symbol_2 = 'KRW-{}'.format(ex_param['symbol'])
    
    binance = gimp.orderbook(ex_1, symbol_1,date_from, date_to)
    upbit = gimp.orderbook(ex_2, symbol_2, date_from, date_to)
    
    
    ex_df1, ex_df2 = gimp.dfParsing(binance, upbit, date_from, maWindow=24, data='1s')  #maWindow는 시간단위
    
    #csv파일로 'C:\\Users\\Public\\Documents\\' 경로에 저장 
    #gimp2.saveDf(ex_df1)
    #gimp2.saveDf(ex_df2)
    
    
    if param['mlflow']:
        #여러 파라미터 대입 분석. mlflow 
        resultDf, pnlDf, buyIndex, sellIndex = gimp.mf(ex_df1,ex_df2,param)
    else:
        #단일 파라미터 
        resultDf, pnlDf, buyIndex, sellIndex = gimp.tradingResult(ex_df1, ex_df2, param)
    
    #plot 그리기 
    #buyIndex sellIndex는 매매했던 시점의 dataframe 인덱스 넘버.  이 인덱스넘버는 plot에서 매매시점 표시할때 사용.
    gimp.plotDf(ex_df1, buyIndex, sellIndex)
    
    return ex_df1, ex_df2, resultDf, pnlDf


def main_ccxt(date_from, date_to, param, ex_param):
        
    #utc 시간으로 넣기
    startTime = date_from
    endTime = date_to   #noww는 현재 utc시간
    
    #ex_1은 외국거래소 , ex_2는 한국거래소 
    ex_1 = ex_param['ex_1']
    symbol_1 = '{}/USDT'.format(ex_param['symbol'])
    
    ex_2 = ex_param['ex_2']
    symbol_2 = '{}/KRW'.format(ex_param['symbol'])
    
    #각 거래소 ohlcv 리턴
    ex_df1 = gimp.ohlcv(ex_1, symbol_1, startTime, endTime)
    ex_df2 = gimp.ohlcv(ex_2, symbol_2, startTime, endTime)
    
    
    #2개 거래소 timestamp 동기화 및 여러 data  dataframe 추가 
    ex_df1, ex_df2 = gimp.dfParsing(ex_df1, ex_df2, startTime, maWindow=24, data='1m')  #maWindow는 시간단위 
    
    #csv파일로 'C:\\Users\\Public\\Documents\\' 경로에 저장 
    #gimp2.saveDf(ex_df1)
    #gimp2.saveDf(ex_df2)
 
    
    if param['mlflow']:
        #여러 파라미터 대입 분석. mlflow 
        resultDf, pnlDf, buyIndex, sellIndex = gimp.mf(ex_df1,ex_df2,param)
    else:
        #단일 파라미터 
        resultDf, pnlDf, buyIndex, sellIndex = gimp.tradingResult(ex_df1, ex_df2, param)
    
    #plot 그리기 
    #buyIndex sellIndex는 매매했던 시점의 dataframe 인덱스 넘버.  이 인덱스넘버는 plot에서 매매시점 표시할때 사용.
    gimp.plotDf(ex_df1, buyIndex, sellIndex)
    
    return ex_df1, ex_df2, resultDf, pnlDf

if __name__ == '__main__':
    
    #단일 파라미터 분석
    param = {'startAssetKrw' : 500000000,
             'startAssetUsd' : 500000,
             'spreadInFrom' : -1,
             'spreadAddIn' : -0.1,
             'spreadOutFrom' : 0.1,
             'slippage': 0.1,
             'count' : 3,
             'mlflow': False
             }
    
    #파라미터 변경 분석
    param_mlflow =  {'startAssetKrw' : 500000000,
                     'startAssetUsd' : 500000,
                     'spreadInFrom' : -1,
                     'spreadInTo' : -0.5,
                     'spreadAddIn' : -0.1,
                     'spreadOutFrom' : 0.1,
                     'spreadOutTo' : 0.3,
                     'spreadInDelta' : 0.02,
                     'spreadOutDelta' : 0.02,
                     'slippage': 0.1,
                     'count' : 3,
                     'mlflow' : True
                     }
    
    ex_param = {'ex_1' : 'binance',     #외국거래소
                'ex_2' : 'upbit',       #한국거래소
                'symbol': 'BTC'}
    
    date_from = '2022-03-26'
    date_to = '2022-03-30'
    
    #빠른 분석 ohlcv 1분data
    #ex_df1, ex_df2, resultDf, pnlDf = main_ccxt(date_from, date_to, param_mlflow, ex_param)
    
    #정밀 분석 orderbook 1초data
    ex_df1, ex_df2, resultDf, pnlDf = main_aws(date_from, date_to, param_mlflow, ex_param)
    
    
    
    