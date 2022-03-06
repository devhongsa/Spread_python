import ccxt

binance = getattr(ccxt, 'binance')()
upbit = getattr(ccxt, 'upbit')()

binance.options['defaultType'] = 'future'

ohlcv = binance.fetch_ohlcv('BTC/USDT','1m')

ticker = upbit.fetch_tickers()


print(ticker.keys())


