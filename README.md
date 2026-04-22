# COT

Local COT explorer built on top of `cot_reports`.

## What It Does

- Fetches weekly COT data from CFTC using `cot_reports`
- Focuses on non-commercial long, short, and net positioning
- Groups markets into categories such as Financial, Metals, Grains, Softs, Livestock, Petroleum and Products, and Natural Gas and Products
- Shows a shared-time-axis chart with COT positioning on top and product price below

## Key Insights

- Extreme positioning often coincides with market turning points (exploratory observation)
- Divergence between positioning and price may indicate weakening trends

## Analytical Objective

This project aims to explore the relationship between market positioning (COT data) and price dynamics.

The goal is to identify whether extreme positioning levels can provide signals about potential market reversals or trend continuation.


## Active Categories

- Natural Gas and Products
- Petroleum and Products
- Agriculture
- Financial
- Indices
- Metals
- Crypto

## Files

- [COT/backend/app.py](COT/backend/app.py): Flask API and static frontend host
- [COT/backend/cot_service.py](COT/backend/cot_service.py): COT fetch, normalization, cache, and price series logic
- [COT/backend/market_catalog.py](COT/backend/market_catalog.py): Category rules and price ticker mapping
- [COT/frontend/index.html](COT/frontend/index.html): Local dashboard UI with Plotly charts






- ## Run Local

1. Start the app with [COT/START.bat](COT/START.bat)
2. The batch file starts the backend and opens `http://127.0.0.1:5056` automatically
