import yfinance as yf

for t in ['DX.f','DXY=F','DX','USDX','DXY','DX-Y.NYB']:
    try:
        df = yf.download(t, period='1y', interval='1wk', progress=False)
        print(t, len(df), df.empty)
    except Exception as e:
        print(t, 'err', e)
