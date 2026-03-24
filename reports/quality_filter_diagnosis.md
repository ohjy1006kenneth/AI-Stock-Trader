# Quality Filter Diagnosis

## Current code thresholds
- net_margin_min: 0.15
- debt_to_equity_max: 0.5
- revenue_growth_min: 0.08
- average_volume_min: 1000000
- market_cap_min: 5000000000
- free_cash_flow_positive: true
- must be U.S. common equity

## Per-ticker rule results (current/original thresholds)
### AAPL
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.27037, threshold>0.15)
- debt_to_equity: FAIL (value=102.63, threshold<0.5)
- free_cash_flow_positive: PASS (value=106312753152, threshold>0)
- revenue_growth: PASS (value=0.157, threshold>0.08)
- liquidity: PASS (value=46586027, threshold>=1000000)
- market_cap: PASS (value=3709462642688, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### MSFT
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.39044, threshold>0.15)
- debt_to_equity: FAIL (value=31.539, threshold<0.5)
- free_cash_flow_positive: PASS (value=53640626176, threshold>0)
- revenue_growth: PASS (value=0.167, threshold>0.08)
- liquidity: PASS (value=34174708, threshold>=1000000)
- market_cap: PASS (value=2769898438656, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### GOOGL
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.32810003, threshold>0.15)
- debt_to_equity: FAIL (value=16.133, threshold<0.5)
- free_cash_flow_positive: PASS (value=38088376320, threshold>0)
- revenue_growth: PASS (value=0.18, threshold>0.08)
- liquidity: PASS (value=32649994, threshold>=1000000)
- market_cap: PASS (value=3538372526080, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### AMZN
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.108339995, threshold>0.15)
- debt_to_equity: FAIL (value=43.435, threshold<0.5)
- free_cash_flow_positive: PASS (value=23793125376, threshold>0)
- revenue_growth: PASS (value=0.136, threshold>0.08)
- liquidity: PASS (value=48889476, threshold>=1000000)
- market_cap: PASS (value=2224919674880, threshold>=5000000000)
- failed_rules: net_margin, debt_to_equity
- total_failed_rules: 2

### META
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.30084, threshold>0.15)
- debt_to_equity: FAIL (value=39.164, threshold<0.5)
- free_cash_flow_positive: PASS (value=23432374272, threshold>0)
- revenue_growth: PASS (value=0.238, threshold>0.08)
- liquidity: PASS (value=14442864, threshold>=1000000)
- market_cap: PASS (value=1499950481408, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### NVDA
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.55603004, threshold>0.15)
- debt_to_equity: FAIL (value=7.255, threshold<0.5)
- free_cash_flow_positive: PASS (value=58128998400, threshold>0)
- revenue_growth: PASS (value=0.732, threshold>0.08)
- liquidity: PASS (value=174903088, threshold>=1000000)
- market_cap: PASS (value=4241587175424, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### LLY
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.31667, threshold>0.15)
- debt_to_equity: FAIL (value=165.31, threshold<0.5)
- free_cash_flow_positive: PASS (value=1951000064, threshold>0)
- revenue_growth: PASS (value=0.426, threshold>0.08)
- liquidity: PASS (value=3071144, threshold>=1000000)
- market_cap: PASS (value=805431869440, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### V
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.50233, threshold>0.15)
- debt_to_equity: FAIL (value=54.612, threshold<0.5)
- free_cash_flow_positive: PASS (value=22032250880, threshold>0)
- revenue_growth: PASS (value=0.146, threshold>0.08)
- liquidity: PASS (value=7744538, threshold>=1000000)
- market_cap: PASS (value=586278240256, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### MA
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.45646998, threshold>0.15)
- debt_to_equity: FAIL (value=256.042, threshold<0.5)
- free_cash_flow_positive: PASS (value=16269375488, threshold>0)
- revenue_growth: PASS (value=0.176, threshold>0.08)
- liquidity: PASS (value=3793413, threshold>=1000000)
- market_cap: PASS (value=446424121344, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### COST
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.029860001, threshold>0.15)
- debt_to_equity: FAIL (value=25.976, threshold<0.5)
- free_cash_flow_positive: PASS (value=6690375168, threshold>0)
- revenue_growth: PASS (value=0.092, threshold>0.08)
- liquidity: PASS (value=2252254, threshold>=1000000)
- market_cap: PASS (value=432808198144, threshold>=5000000000)
- failed_rules: net_margin, debt_to_equity
- total_failed_rules: 2

### ADBE
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.29477, threshold>0.15)
- debt_to_equity: FAIL (value=58.217, threshold<0.5)
- free_cash_flow_positive: PASS (value=9292624896, threshold>0)
- revenue_growth: PASS (value=0.12, threshold>0.08)
- liquidity: PASS (value=5712062, threshold>=1000000)
- market_cap: PASS (value=98088976384, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### CRM
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.17958, threshold>0.15)
- debt_to_equity: FAIL (value=29.947, threshold<0.5)
- free_cash_flow_positive: PASS (value=16366999552, threshold>0)
- revenue_growth: PASS (value=0.121, threshold>0.08)
- liquidity: PASS (value=12021908, threshold>=1000000)
- market_cap: PASS (value=172553240576, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### ISRG
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.28375998, threshold>0.15)
- debt_to_equity: FAIL (value=0.953, threshold<0.5)
- free_cash_flow_positive: PASS (value=2274837504, threshold>0)
- revenue_growth: PASS (value=0.188, threshold>0.08)
- liquidity: PASS (value=1867700, threshold>=1000000)
- market_cap: PASS (value=166787694592, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### ABBV
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.0691, threshold>0.15)
- debt_to_equity: FAIL (value=None, threshold<0.5)
- free_cash_flow_positive: PASS (value=18337748992, threshold>0)
- revenue_growth: PASS (value=0.1, threshold>0.08)
- liquidity: PASS (value=7058505, threshold>=1000000)
- market_cap: PASS (value=361581707264, threshold>=5000000000)
- failed_rules: net_margin, debt_to_equity
- total_failed_rules: 2

### SPGI
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.29154, threshold>0.15)
- debt_to_equity: FAIL (value=37.912, threshold<0.5)
- free_cash_flow_positive: PASS (value=4877749760, threshold>0)
- revenue_growth: PASS (value=0.09, threshold>0.08)
- liquidity: PASS (value=2526379, threshold>=1000000)
- market_cap: PASS (value=125742243840, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### MSCI
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.38358003, threshold>0.15)
- debt_to_equity: FAIL (value=None, threshold<0.5)
- free_cash_flow_positive: PASS (value=1164579328, threshold>0)
- revenue_growth: PASS (value=0.106, threshold>0.08)
- liquidity: FAIL (value=624964, threshold>=1000000)
- market_cap: PASS (value=39928733696, threshold>=5000000000)
- failed_rules: debt_to_equity, liquidity
- total_failed_rules: 2

### ROP
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: PASS (value=0.19441, threshold>0.15)
- debt_to_equity: FAIL (value=47.957, threshold<0.5)
- free_cash_flow_positive: PASS (value=2118599936, threshold>0)
- revenue_growth: PASS (value=0.097, threshold>0.08)
- liquidity: PASS (value=1627606, threshold>=1000000)
- market_cap: PASS (value=37528330240, threshold>=5000000000)
- failed_rules: debt_to_equity
- total_failed_rules: 1

### TT
- us_stock: FAIL (country=Ireland)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.13688, threshold>0.15)
- debt_to_equity: FAIL (value=63.247, threshold<0.5)
- free_cash_flow_positive: PASS (value=1935837440, threshold>0)
- revenue_growth: FAIL (value=0.056, threshold>0.08)
- liquidity: PASS (value=1577862, threshold>=1000000)
- market_cap: PASS (value=95282356224, threshold>=5000000000)
- failed_rules: us_stock, net_margin, debt_to_equity, revenue_growth
- total_failed_rules: 4

### ETN
- us_stock: FAIL (country=Ireland)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.1489, threshold>0.15)
- debt_to_equity: FAIL (value=54.877, threshold<0.5)
- free_cash_flow_positive: PASS (value=2598000128, threshold>0)
- revenue_growth: PASS (value=0.131, threshold>0.08)
- liquidity: PASS (value=2868613, threshold>=1000000)
- market_cap: PASS (value=143902195712, threshold>=5000000000)
- failed_rules: us_stock, net_margin, debt_to_equity
- total_failed_rules: 3

### UNH
- us_stock: PASS (country=United States)
- supported_quote_type: PASS (quote_type=EQUITY)
- net_margin: FAIL (value=0.02694, threshold>0.15)
- debt_to_equity: FAIL (value=81.618, threshold<0.5)
- free_cash_flow_positive: PASS (value=13863249920, threshold>0)
- revenue_growth: PASS (value=0.123, threshold>0.08)
- liquidity: PASS (value=9503616, threshold>=1000000)
- market_cap: PASS (value=247259971584, threshold>=5000000000)
- failed_rules: net_margin, debt_to_equity
- total_failed_rules: 2

## Bottleneck summary
- net_margin: 6 failures
- debt_to_equity: 20 failures
- revenue_growth: 1 failures
- liquidity: 1 failures
- market_cap: 0 failures
- us_stock: 2 failures
- supported_quote_type: 0 failures
- free_cash_flow_positive: 0 failures
- biggest bottleneck: debt_to_equity (20 of 20 names)

## Why 0 names currently pass
- The debt/equity rule is the blocker. All 20 names fail `debt_to_equity < 0.5` under the current source values.
- That means even otherwise strong companies cannot pass the full screen.
- Additional failures exist, but they are secondary: 6 fail net margin, 1 fails revenue growth, 1 fails liquidity, and 2 are non-U.S. names in the static seed universe.
- Because every candidate fails at least the debt/equity test, the qualified universe is empty by construction.

## Threshold comparison
- old thresholds pass count: 0 -> []
- user starting proposal pass count: 1 -> ['ISRG']
- final recommended V1 thresholds pass count: 7 -> ['MSFT', 'GOOGL', 'META', 'NVDA', 'CRM', 'ISRG', 'SPGI']
- Recommendation rationale: keep net margin and revenue growth selective, keep investability rules unchanged, and relax debt/equity to a source-unit-aware threshold that yields a practical but still selective CORE list.