import MetaTrader5 as mt5

# Test MT5 connection
if mt5.initialize():
    print("MT5 initialized successfully")

    # Get account info
    account = mt5.account_info()
    if account:
        print(f"Account: {account.login}, Server: {account.server}")
    else:
        print("No account info")

    # Get symbols
    symbols = mt5.symbols_get()
    if symbols:
        dxy_symbols = [s.name for s in symbols if 'DX' in s.name.upper() or 'DXY' in s.name.upper() or 'USDX' in s.name.upper()]
        print(f"DXY-like symbols: {dxy_symbols}")
    else:
        print("No symbols retrieved")

    # Try to get DXY info
    for sym in ["DXY", "DX.f", "DXY_M6", "USDX", "DX"]:
        symbol_info = mt5.symbol_info(sym)
        if symbol_info:
            print(f"{sym} symbol info: visible={symbol_info.visible}, path={symbol_info.path}")
        else:
            print(f"{sym} symbol not found")

    mt5.shutdown()
else:
    print("MT5 initialize failed")
    print("MT5 initialize failed")