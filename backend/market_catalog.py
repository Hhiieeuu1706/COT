from __future__ import annotations

from typing import Optional

INDEX_KEYWORDS = [
    "NASDAQ MINI",
    "NASDAQ-100",
    "NASDAQ OVER THE COUNTER INDEX",
    "S&P",
    "DJIA",
    "DOW JONES",
    "RUSSELL",
    "RUSSEL",
    "NIKKEI",
    "STOCK INDEX",
    "STOCK AVERAGE",
    "DIVIDEND INDEX",
    "TOTAL RETURN INDEX",
    "BARRA",
    "E-MINI S&P",
    "E-MINI NASDAQ",
    "MICRO E-MINI NASDAQ",
    "MICRO E-MINI S&P",
    "RUSSELL E-MINI",
]

FINANCIAL_KEYWORDS = [
    "EURO FX",
    "BRITISH POUND",
    "CANADIAN DOLLAR",
    "SWISS FRANC",
    "JAPANESE YEN",
    "AUSTRALIAN DOLLAR",
    "AUSTRALIAN DOLLARS",
    "NZ DOLLAR",
    "MEXICAN PESO",
    "UST ",
    "U.S. TREASURY",
    "TREASURY NOTE",
    "TREASURY NOTES",
    "TREASURY BOND",
    "ULTRA UST",
    "FED FUNDS",
    "SOFR",
    "ERIS SWAP",
    "DELIVERABLE IR",
    "MSCI",
]

CRYPTO_KEYWORDS = [
    "BITCOIN",
    "MICRO BITCOIN",
    "BITCOIN-USD",
    "ETHER",
    "MICRO ETHER",
]

COMMODITY_RULES = [
    ("Natural Gas and Products", ["NAT GAS ICE LD1", "NAT GAS ICE PEN"]),
    ("Petroleum and Products", [
        "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
        "CRUDE OIL, LIGHT 'SWEET' - NEW YORK MERCANTILE EXCHANGE",
        "WTI-PHYSICAL",
        "ETHANOL - NEW YORK MERCANTILE EXCHANGE",
        "ETHANOL T2 FOB INCL DUTY",
        "GASOLINE RBOB",
        "WTI FINANCIAL CRUDE OIL",
    ]),
    ("Agriculture", [
        "WHEAT-SRW",
        "WHEAT-HRW",
        "CORN - CHICAGO BOARD OF TRADE",
        "ROUGH RICE - CHICAGO BOARD OF TRADE",
        "SOYBEANS - CHICAGO BOARD OF TRADE",
        "COFFEE C",
        "SUGAR NO. 11",
        "COCOA - ",
    ]),
    ("Metals", [
        "GOLD - COMMODITY EXCHANGE INC.",
        "SILVER - COMMODITY EXCHANGE INC.",
        "PLATINUM - COMMODITY EXCHANGE INC.",
        "COPPER - COMMODITY EXCHANGE INC.",
        "COPPER- #1 - COMMODITY EXCHANGE INC.",
        "COPPER-GRADE #1 - COMMODITY EXCHANGE INC.",
    ]),
]

CATEGORY_ORDER = {
    "Natural Gas and Products": 0,
    "Petroleum and Products": 1,
    "Agriculture": 2,
    "Financial": 3,
    "Electricity": 4,
    "Metals and Other": 5,
    "Other": 99,
}

# Strict taxonomy requested by user. Only mapped markets are included in catalog.
STRICT_CATEGORY_LABELS = {
    "Financial": [
        "CANADIAN DOLLAR", "SWISS FRANC", "BRITISH POUND", "JAPANESE YEN", "EURO FX",
        "EURO FX/BRITISH POUND XRATE", "AUSTRALIAN DOLLAR", "MEXICAN PESO", "BRAZILIAN REAL",
        "NZ DOLLAR", "SO AFRICAN RAND", "DJIA CONSOLIDATED", "DJIA X 5", "DOW JONES U.S. REAL ESTATE IDX",
        "MICRO E-MINI DJIA", "S&P 500 CONSOLIDATED", "E-MINI S&P CONSU STAPLES INDEX", "E-MINI S&P ENERGY INDEX",
        "E-MINI S&P 500", "E-MINI S&P FINANCIAL INDEX", "E-MINI S&P HEALTH CARE INDEX",
        "E-MINI S&P MATERIALS INDEX", "E-MINI S&P TECHNOLOGY INDEX", "E-MINI S&P UTILITIES INDEX",
        "MICRO E-MINI S&P 500 INDEX", "ADJUSTED INT RATE S&P 500 TOTL", "E-MINI S&P 400 STOCK INDEX",
        "NASDAQ-100 CONSOLIDATED", "NASDAQ MINI", "MICRO E-MINI NASDAQ-100 INDEX", "RUSSELL E-MINI",
        "EMINI RUSSELL 1000 VALUE INDEX", "MICRO E-MINI RUSSELL 2000 INDX", "NIKKEI STOCK AVERAGE YEN DENOM",
        "MSCI EAFE", "MSCI EM INDEX", "S&P 500 ANNUAL DIVIDEND INDEX", "S&P 500 QUARTERLY DIVIDEND IND",
        "UST BOND", "ULTRA UST BOND", "UST 2Y NOTE", "UST 10Y NOTE", "ULTRA UST 10Y", "UST 5Y NOTE",
        "FED FUNDS", "EURO SHORT TERM RATE", "SOFR-3M", "SOFR-1M", "2 YEAR ERIS SOFR SWAP",
        "10 YEAR ERIS SOFR SWAP", "5 YEAR ERIS SOFR SWAP", "BITCOIN", "MICRO BITCOIN", "NANO BITCOIN",
        "ETHER CASH SETTLED", "MICRO ETHER", "NANO ETHER", "DOGECOIN", "AVALANCHE", "USD INDEX",
        "VIX FUTURES", "BBG COMMODITY",
    ],
    "Agriculture": [
        "WHEAT-SRW", "WHEAT-HRW", "WHEAT-HRSPRING", "CORN", "ROUGH RICE", "LEAN HOGS", "LIVE CATTLE",
        "FEEDER CATTLE", "BUTTER", "MILK, CLASS III", "NON FAT DRY MILK", "CME MILK IV", "CHEESE",
        "SOYBEANS", "SOYBEAN OIL", "SOYBEAN MEAL", "USD MALAYSIAN CRUDE PALM OIL C", "CANOLA",
        "COTTON NO. 2", "FRZN CONCENTRATED ORANGE JUICE", "COCOA", "SUGAR NO. 11", "COFFEE C",
    ],
    "Petroleum and Products": [
        "USGC HSFO", "FUEL OIL-3% USGC/3.5% FOB RDAM", "USGC HSFO-PLATTS/BRENT 1ST LN", "NY HARBOR ULSD",
        "UP DOWN GC ULSD VS HO SPR", "ETHANOL T2 FOB INCL DUTY", "ETHANOL", "CRUDE DIFF-WCS HOUSTON/WTI 1ST",
        "CRUDE OIL, LIGHT SWEET-WTI", "CRUDE OIL, LIGHT SWEET", "CRUDE DIFF-TMX WCS 1A INDEX",
        "CRUDE DIFF-TMX SW 1A INDEX", "CONDENSATE DIF-TMX C5 1A INDEX", "WTI-PHYSICAL", "WTI FINANCIAL CRUDE OIL",
        "BRENT LAST DAY", "WTI HOUSTON ARGUS/WTI TR MO", "WTI MIDLAND ARGUS VS WTI TRADE", "GASOLINE RBOB",
        "GULF COAST CBOB GAS A2 PL RBOB", "GULF JET NY HEAT OIL SPR", "MARINE .5% FOB USGC/BRENT 1ST",
        "GULF # 6 FUEL OIL CRACK",
    ],
    "Natural Gas and Products": [
        "NAT GAS ICE LD1", "NAT GAS ICE PEN", "SOCAL BORDER FIN BASIS", "PG&E CITYGATE FIN BASIS",
        "NWP ROCKIES FIN BASIS", "AECO FIN BASIS", "CHICAGO FIN BASIS", "HSC FIN BASIS", "WAHA FIN BASIS",
        "ALGONQUIN CITYGATES BASIS", "CHICAGO CITYGATE", "CIG ROCKIES BASIS", "TCO BASIS", "CG MAINLINE BASIS",
        "DOMINION - SOUTH POINT", "EP SAN JUAN BASIS", "HENRY HUB BASIS", "HENRY HUB INDEX",
        "HOUSTON SHIP CHANNEL", "MICHCON FINANCIAL INDEX", "MICHCON BASIS", "NAT GAS LD1 FOR GDD -TEXOK",
        "NGPL MIDCONT BASIS", "NGPL TXOK BASIS", "NNG VENTURA BASIS", "NWP ROCKIES INDEX", "PANHANDLE BASIS",
        "SONAT - TIER 1 POOL", "TETCO M3 INDEX", "TETCO M3 BASIS", "TGT ZONE 1 BASIS", "TRANSCO STN 85 MONTHLY INDEX",
        "TRANSCO ZONE 6 BASIS", "TRANSCO ZONE 6 MONTHLY INDEX", "TRANSCO STATION 85-ZONE 4 BASI",
        "WAHA INDEX", "TETCO M2 BASIS", "TRANSCO LEIDY BASIS", "TETCO M2 INDEX", "REX ZONE 3 BASIS",
        "REX ZONE 3 INDEX", "TRANSCO ZONE 5 SOUTH BASIS", "NAT GAS NYME", "HENRY HUB LAST DAY FIN",
        "HENRY HUB PENULTIMATE FIN", "HENRY HUB", "HENRY HUB PENULTIMATE NAT GAS", "PROPANE ARGUS FAR EAST MINI",
        "ARGUS CIF ARA LG FINL PROPANE", "ARGUS FAR EAST PROPANE", "ETHANE, MT. BELV-ENTERPRISE",
        "NAT GASLNE OPIS MT B NONTET FP", "BUTANE OPIS MT BELV NONTET FP", "PROPANE OPIS CONWAY INWELL FP",
        "PROPANE OPIS MT BELV NONTET FP", "PROPANE OPIS MT BELVIEU TET FP", "PROPANE ARGUS SAUDI CP FP",
        "ARGUS PROPANE FAR EAST INDEX", "PROPANE NON-LDH MT BEL", "PROPANE", "MT BELVIEU ETHANE OPIS",
        "MT BELV NORM BUTANE OPIS", "MT BELV NAT GASOLINE OPIS", "CONWAY PROPANE",
    ],
    "Electricity": [
        "MID-C DAY-AHEAD PEAK", "MID-C DAY-AHEAD OFF-PEAK", "PJM WESTERN HUB RT OFF", "SP15 FIN DA PEAK FIXED",
        "CAISO SP-15 DA OFF-PK FIXED", "PJM AEP DAYTON RT OFF-PK FIXED", "PJM AEP DAYTON RT PEAK FIXED",
        "PJM AEP DAYTON HUB DA OFF-PK", "PJM AEP DAYTON DA PEAK", "MISO INDIANA OFF-PEAK", "MISO IN. REAL-TIME OFF-PEAK",
        "ERCOT HOUSTON 345KV RT PK FIX", "ERCOT HOUSTON 345KV RT OFF FIX", "ERCOT NORTH 345KV RT PK FIX",
        "ERCOT SOUTH 345 KV RT PEAK FIX", "ERCOT-SOUTH MONTHLY OFFPEAK", "ERCOT WEST 345KV RT PEAK",
        "ERCOT WEST 345K RT OFF-PEAK", "ERCOT - NORTH MONTHLY OFF-PEAK", "MISO IN. DAY-AHEAD PEAK",
        "MISO INDIANA HUB RT PEAK", "PJM N. IL HUB DA OFF-PK", "PJM N. IL HUB DA PEAK", "ISO NE MASS HUB DA OFF-PK FIXD",
        "ISO NE MASS HUB DA PEAK", "CAISO NP-15 PEAK", "CAISO NP-15 DA OFF-PK FIXED", "PJM NI HUB RT OFF-PK FIXED",
        "PJM N. IL HUB RT PEAK", "NYISO ZONE A DA PEAK", "NYISO ZONE A DA OFF-PK FIX PR", "NYISO ZONE G DA PEAK",
        "NYISO ZONE G DA OFF-PK", "PALO VERDE DA PEAK", "PALO VERDE DA OFF-PK FIXED PR", "PJM PPL ZONE DA OFF-PEAK FIXED",
        "PJM PPL ZONE DA PEAK", "PJM WESTERN HUB DA OFF-PK", "PJM WESTERN HUB DA PEAK", "PJM WESTERN HUB RT PEAK MINI",
        "ERCOT N 345KV REAL T PK DALY M", "ERCOT N 345KV REAL T PK 2X16", "ERCOT NORTH 345KV HUB RT 7X8",
        "ISONE.H.INTERNAL_HUB_MO_ON_DAP", "ISONE.H.INTERNAL_HB_MO_OFF_DAP", "MISO.INDIANA.HUB_MONTH_ON_DAP",
        "NYISO.HUD VL_MONTH_ON_DAP", "NYISO.HUD VL_MONTH_OFF_DAP", "NYISO.N.Y.C._MONTH_ON_DAP", "NYISO.N.Y.C._MONTH_OFF_DAP",
        "NYISO.WEST_MONTH_ON_DAP", "NYISO.WEST_MONTH_OFF_DAP", "PJM.AEP_MONTH_ON_DAP", "PJM.AEP-DAYTON HUB_MO_ON_DAP",
        "PJM.AEP-DAYTON HUB_MO_OFF_DAP", "PJM.BGE_MONTH_ON_DAP", "PJM.BGE_MONTH_OFF_DAP", "PJM.JCPL_MONTH_ON_DAP",
        "PJM.JCPL_MONTH_OFF_DAP", "PJM.METED_MONTH_ON_DAP", "PJM.METED_MONTH_OFF_DAP", "PJM.N ILLINOIS HUB_MO_ON_DAP",
        "PJM.N ILLINOIS HUB_MO_OFF_DAP", "PJM.PECO_MONTH_ON_DAP", "PJM.PECO_MONTH_OFF_DAP", "PJM.PEPCO_MONTH_ON_DAP",
        "PJM.PEPCO_MONTH_OFF_DAP", "PJM.PPL_MONTH_ON_DAP", "PJM.PPL_MONTH_OFF_DAP", "PJM.PSEG_MONTH_ON_DAP",
        "PJM.PSEG_MONTH_OFF_DAP", "PJM.WESTERN HUB_MONTH_ON_DAP", "PJM.WESTERN HUB_MONTH_OFF_DAP",
        "MISO.INDIANA.HUB_MONTH_OFF_RTP", "PJM.AEP-DAYTON HUB_MO_ON_RTP", "PJM.AEP-DAYTON HUB_MO_OFF_RTP",
        "PJM.N ILLINOIS HUB_MO_ON_RTP", "PJM.N ILLINOIS HUB_MO_OFF_RTP", "PJM.WESTERN HUB_MONTH_ON_RTP",
        "PJM.WESTERN HUB_MONTH_OFF_RTP", "ERCOT.HB_NORTH_MONTH_ON_RTP", "ERCOT.HB_NORTH_MONTH_OFF_RTP",
        "ERCOT NORTH 345KV HUB RT 7X16",
    ],
    "Metals and Other": [
        "PALLADIUM", "PLATINUM", "SILVER", "GOLD", "MICRO GOLD", "COPPER- #1", "COBALT", "LITHIUM HYDROXIDE",
        "ALUMINUM MWP", "ALUMINIUM EURO PREM DUTY-PAID", "STEEL-HRC", "NORTH EURO HOT-ROLL COIL STEEL", "LUMBER",
        "NJ SRECS", "PJM TRI-RECS CLASS 1", "CALIF LOW CARBON FSC-OPIS", "D6 RINS OPIS CURRENT YEAR",
        "PA SOLAR ALTER ENERGY CREDIT", "CALIF CARBON CURRENT AUCTION", "D4 BIODIESEL RINS OPIS CURR YR",
        "MASS COMPLIANCE RECS CLASS 1", "NEPOOL DUAL RECS CLASS 1", "PA COMPLIANCE AECS TIER1",
        "CALIF CARBON ALL VINTAGE 2026", "TX GREEN-E REC V25 BACK HALF", "TX REC CRS V26 BACK HALF",
        "TX REC CRS V27 BACK HALF", "TX REC CRS V28 BACK HALF", "TX REC CRS V29 BACK HALF",
        "TX REC CRS V26 FRONT HALF", "TX REC CRS V27 FRONT HALF", "TX REC CRS V28 FRONT HALF",
        "TX REC CRS V29 FRONT HALF", "PENNSYLVANIA AEC TIER 2-V2026",
    ],
}


def _norm_label(text: str) -> str:
    return "".join(ch for ch in text.upper() if ch.isalnum())


STRICT_CATEGORY_NORMS = {
    category: {_norm_label(label) for label in labels}
    for category, labels in STRICT_CATEGORY_LABELS.items()
}

PRICE_TICKERS = {
    "CANADIAN DOLLAR": "6C=F",
    "SWISS FRANC": "6S=F",
    "BRITISH POUND": "6B=F",
    "JAPANESE YEN": "6J=F",
    "EURO FX": "6E=F",
    "AUSTRALIAN DOLLAR": "6A=F",
    "AUSTRALIAN DOLLARS": "6A=F",
    "NZ DOLLAR": "6N=F",
    "MEXICAN PESO": "6M=F",
    "BITCOIN": "BTC-USD",
    "MICRO BITCOIN": "BTC-USD",
    "BITCOIN-USD": "BTC-USD",
    "ETHER": "ETH-USD",
    "ETHER CASH SETTLED": "ETH-USD",
    "MICRO ETHER": "ETH-USD",
    "GOLD - COMMODITY EXCHANGE INC.": "GC=F",
    "SILVER - COMMODITY EXCHANGE INC.": "SI=F",
    "PLATINUM - COMMODITY EXCHANGE INC.": "PL=F",
    "COPPER - COMMODITY EXCHANGE INC.": "HG=F",
    "COPPER- #1 - COMMODITY EXCHANGE INC.": "HG=F",
    "COPPER-GRADE #1 - COMMODITY EXCHANGE INC.": "HG=F",
    "NAT GAS ICE LD1": "NG=F",
    "NAT GAS ICE PEN": "NG=F",
    "CRUDE OIL, LIGHT SWEET": "CL=F",
    "WTI-PHYSICAL": "CL=F",
    "ETHANOL": "EH=F",
    "GASOLINE RBOB": "RB=F",
    "WTI FINANCIAL CRUDE OIL": "CL=F",
    "WHEAT-SRW": "ZW=F",
    "WHEAT-HRW": "KE=F",
    "CORN - CHICAGO BOARD OF TRADE": "ZC=F",
    "ROUGH RICE - CHICAGO BOARD OF TRADE": "ZR=F",
    "SOYBEANS - CHICAGO BOARD OF TRADE": "ZS=F",
    "COFFEE C": "KC=F",
    "SUGAR NO. 11": "SB=F",
    "COCOA": "CC=F",
    "USD INDEX - ICE FUTURES U.S.": "DXY=F",  # Tries yfinance first, then investpy fallback
}

ALIAS_TICKERS = [
    ("MICRO E-MINI NASDAQ", "MNQ=F"),
    ("NASDAQ MINI", "NQ=F"),
    ("NASDAQ-100", "NQ=F"),
    ("NASDAQ 100", "NQ=F"),
    ("NASDAQ OVER THE COUNTER INDEX", "NQ=F"),
    ("MICRO E-MINI S&P 500", "MES=F"),
    ("E-MINI S&P 500", "ES=F"),
    ("E-MINI S&P ", "ES=F"),
    ("S&P 500", "ES=F"),
    ("S&P 400", "ES=F"),
    ("DJIA", "YM=F"),
    ("DOW JONES", "YM=F"),
    ("MICRO E-MINI RUSSELL 2000", "M2K=F"),
    ("RUSSELL 2000", "RTY=F"),
    ("RUSSEL 2000", "RTY=F"),
    ("RUSSEL 3000", "RTY=F"),
    ("RUSSELL E-MINI", "RTY=F"),
    ("RUSSELL 1000 VALUE", "EFA"),  # Changed from ^RUI (invalid) to EFA
    ("EMINI RUSSELL 1000 GROWTH", "IWF"),
    ("NIKKEI", "NKD=F"),
    ("UST 2Y NOTE", "ZT=F"),
    ("2 YEAR U.S. TREASURY NOTES", "ZT=F"),
    ("2-YEAR U.S. TREASURY NOTES", "ZT=F"),
    ("UST 5Y NOTE", "ZF=F"),
    ("5 YEAR U.S. TREASURY NOTES", "ZF=F"),
    ("5-YEAR U.S. TREASURY NOTES", "ZF=F"),
    ("UST 10Y NOTE", "ZN=F"),
    ("10 YEAR", "ZN=F"),
    ("10-YEAR U.S. TREASURY NOTES", "ZN=F"),
    ("13-WEEK U.S. TREASURY BILLS", None),  # Changed from ^IRX (invalid) to None - no direct yfinance mapping
    ("U.S. TREASURY BILLS", None),  # Changed from ^IRX to None
    ("UST BOND", "ZB=F"),
    ("U.S. TREASURY BONDS", "ZB=F"),
    ("LONG-TERM U.S. TREASURY BONDS", "ZB=F"),
    ("LONG TERM U.S. TREASURY BONDS", "ZB=F"),
    ("ULTRA UST BOND", "UB=F"),
    ("ULTRA UST 10Y", "TN=F"),
    ("ULTRA U.S. TREASURY BONDS", "UB=F"),
    ("FED FUNDS", "ZQ=F"),
    ("1-MONTH SOFR", "SR1=F"),
    ("SOFR-1M", "SR1=F"),
    ("3-MONTH SOFR", "SR3=F"),
    ("SOFR-3M", "SR3=F"),
    ("ERIS SOFR SWAP", "SR3=F"),
    ("DELIVERABLE IR", "ZN=F"),
    ("MSCI", "EFA"),
]


def slugify_market_name(name: str) -> str:
    cleaned = []
    previous_dash = False
    for char in name.lower():
        if char.isalnum():
            cleaned.append(char)
            previous_dash = False
        elif not previous_dash:
            cleaned.append("-")
            previous_dash = True
    slug = "".join(cleaned).strip("-")
    return slug


def categorize_market(name: str) -> str:
    upper_name = name.upper()
    if any(keyword in upper_name for keyword in CRYPTO_KEYWORDS):
        return "Crypto"
    if any(keyword in upper_name for keyword in INDEX_KEYWORDS):
        return "Indices"
    if any(keyword in upper_name for keyword in FINANCIAL_KEYWORDS):
        return "Financial"
    for category, keywords in COMMODITY_RULES:
        if any(keyword in upper_name for keyword in keywords):
            return category
    return "Other"


def categorize_market_strict(name: str) -> Optional[str]:
    upper_name = name.upper().strip()
    base_name = upper_name.split(" - ")[0].strip() if " - " in upper_name else upper_name
    full_norm = _norm_label(upper_name)
    base_norm = _norm_label(base_name)

    for category, label_norms in STRICT_CATEGORY_NORMS.items():
        for label_norm in label_norms:
            if not label_norm:
                continue
            if base_norm == label_norm or full_norm == label_norm:
                return category
            if len(label_norm) >= 8 and (label_norm in full_norm or label_norm in base_norm):
                return category
            if len(base_norm) >= 8 and base_norm in label_norm:
                return category
    return None


def categorize_market_with_fallback(name: str) -> str:
    category = categorize_market(name)
    if category != "Other":
        return category

    # Auto-create a readable category from the exchange suffix when present.
    if " - " in name:
        suffix = name.split(" - ")[-1].strip()
        if suffix:
            return f"Other: {suffix.title()}"

    first_token = name.strip().split(" ")[0].strip("-_/,.()") if name.strip() else ""
    if first_token:
        return f"Other: {first_token.title()}"
    return "Other: Uncategorized"


def infer_price_ticker(name: str) -> Optional[str]:
    upper_name = name.upper()
    if upper_name in PRICE_TICKERS:
        return PRICE_TICKERS[upper_name]
    for known_name, ticker in PRICE_TICKERS.items():
        if known_name in upper_name or upper_name in known_name:
            return ticker
    for keyword, ticker in ALIAS_TICKERS:
        if ticker and keyword in upper_name:  # Skip if ticker is None
            return ticker
    return None


def category_sort_key(name: str) -> tuple[int, str]:
    return CATEGORY_ORDER.get(name, 50), name
