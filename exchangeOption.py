import ccxt 

def exchangeOption(exchange):
    if exchange == 'binance':
        Exchange = getattr(ccxt, exchange)()
        Exchange.options['defaultType'] = 'future'
        return Exchange
    else:
        return getattr(ccxt,exchange)()