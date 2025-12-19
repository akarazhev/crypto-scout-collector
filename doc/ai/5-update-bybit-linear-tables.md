# Issue 5: Update bybit linear tables

In this `crypto-scout-collector-db` project we are going to check and update the following bybit linear tables to save
the data received from the Bybit websocket: `bybit_linear_tickers`, `bybit_linear_kline_1m`, `bybit_linear_kline_5m`,
`bybit_linear_kline_15m`, `bybit_linear_kline_60m`, `bybit_linear_kline_240m`, `bybit_linear_kline_1d`,
`bybit_linear_public_trade`, `bybit_linear_order_book_1`, `bybit_linear_order_book_50`,`bybit_linear_order_book_200`,
`bybit_linear_order_book_1000`, `bybit_linear_all_liquidation`. Table schemas must be in normalized form and optimal to
perform analysis and efficient saving the data. Data removal and compression must be done with: `retention` and
`compression` policies.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Use the following technical stack: `timescale/timescaledb:latest-pg17`.
- Use human-readable names for parameter names.
- Be sure that all parameters are preset in the table schemas.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` review the current script implementations: `init.sql`, `bybit_linear_tables.sql` in
  `crypto-scout-collector-db` project and update it by adding missing parameters for defined tables.
- As the `expert database engineer` recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the `expert database engineer` rely on the definition of the data section.

## Definition of the data

### Bybit linear tickers data

The `ticker` data received from the Bybit websocket is the following:

Linear Perpetual:
```json
{
  "topic": "tickers.BTCUSDT",
  "type": "snapshot",
  "data": {
    "symbol": "BTCUSDT",
    "tickDirection": "MinusTick",
    "price24hPcnt": "-0.158315",
    "lastPrice": "66666.60",
    "prevPrice24h": "79206.20",
    "highPrice24h": "79266.30",
    "lowPrice24h": "65076.90",
    "prevPrice1h": "66666.60",
    "markPrice": "66666.60",
    "indexPrice": "115418.19",
    "openInterest": "492373.72",
    "openInterestValue": "32824881841.75",
    "turnover24h": "4936790807.6521",
    "volume24h": "73191.3870",
    "fundingIntervalHour": "8",
    "fundingCap": "0.005",
    "nextFundingTime": "1760342400000",
    "fundingRate": "-0.005",
    "bid1Price": "66666.60",
    "bid1Size": "23789.165",
    "ask1Price": "66666.70",
    "ask1Size": "23775.469",
    "preOpenPrice": "",
    "preQty": "",
    "curPreListingPhase": ""
  },
  "cs": 9532239429,
  "ts": 1760325052630
}
```

Linear Futures:
```json
{
  "topic": "tickers.BTC-26DEC25",
  "type": "snapshot",
  "data": {
    "symbol": "BTC-26DEC25",
    "tickDirection": "ZeroMinusTick",
    "price24hPcnt": "0",
    "lastPrice": "109401.50",
    "prevPrice24h": "109401.50",
    "highPrice24h": "109401.50",
    "lowPrice24h": "109401.50",
    "prevPrice1h": "109401.50",
    "markPrice": "121144.63",
    "indexPrice": "114132.51",
    "openInterest": "6.622",
    "openInterestValue": "802219.74",
    "turnover24h": "0.0000",
    "volume24h": "0.0000",
    "deliveryTime": "2025-12-26T08:00:00Z",
    "basisRate": "0.06129209",
    "deliveryFeeRate": "0",
    "predictedDeliveryPrice": "0.00",
    "basis": "-4730.84",
    "basisRateYear": "0.30655351",
    "nextFundingTime": "",
    "fundingRate": "",
    "bid1Price": "111254.50",
    "bid1Size": "0.176",
    "ask1Price": "131001.00",
    "ask1Size": "0.580"
  },
  "cs": 31337927919,
  "ts": 1760409119857
}
```

Parameters to save:

- `cs`: integer. Cross sequence.
- `ts`: number. The timestamp (ms) that the system generates the data.
- `data`: array. Object.
- `symbol`: string. Symbol name.
- `tickDirection`: string. Tick direction.
- `price24hPcnt`: string. Percentage change of market price in the last 24 hours.
- `lastPrice`: string. Last price.
- `prevPrice24h`: string. Market price 24 hours ago.
- `highPrice24h`: string. The highest price in the last 24 hours.
- `lowPrice24h`: string. The lowest price in the last 24 hours.
- `prevPrice1h`: string. Market price an hour ago.
- `markPrice`: string. Mark price.
- `indexPrice`: string. Index price.
- `openInterest`: string. Open interest size.
- `openInterestValue`: string. Open interest value.
- `turnover24h`: string. Turnover for 24h.
- `volume24h`: string. Volume for 24h.
- `nextFundingTime`: string. Next funding timestamp (ms).
- `fundingRate`: string. Funding rate.
- `bid1Price`: string. Best bid price.
- `bid1Size`: string. Best bid size.
- `ask1Price`: string. Best ask price.
- `ask1Size`: string. Best ask size.
- `deliveryTime`: datetime. Delivery date time (UTC+0), applicable to expired futures only.
- `basisRate`: string. Basis rate. Unique field for inverse futures & USDT/USDC futures.
- `deliveryFeeRate`: string. Delivery fee rate. Unique field for inverse futures & USDT/USDC futures.
- `predictedDeliveryPrice`: string. Predicated delivery price. Unique field for inverse futures & USDT/USDC futures.
- `preOpenPrice`: string. Estimated pre-market contract open price: 
  - The value is meaningless when entering continuous trading phase 
  - USDC Futures and Inverse Futures do not have this field
- `preQty`: string. Estimated pre-market contract open qty:
  - The value is meaningless when entering continuous trading phase
  - USDC Futures and Inverse Futures do not have this field
- `curPreListingPhase`: string. The current pre-market contract phase: 
  - USDC Futures and Inverse Futures do not have this field.
- `fundingIntervalHour`: string. Funding interval hour
  - This value currently only supports whole hours
  - Only for Perpetual,For Futures,this field will not return
- `fundingCap`: string. Funding rate upper and lower limits
  - Only for Perpetual,For Futures,this field will not return.
- `basisRateYear`: string. Annual basis rate
  - Only for Futures,For Perpetual,this field will not return.

### Bybit linear klines data

The `kline` data received from the Bybit websocket is the following:

```json
{
  "topic": "kline.15.BTCUSDT",
  "data": [
    {
      "start": 1761670800000,
      "end": 1761674399999,
      "interval": "60",
      "open": "1362338.8",
      "close": "1386000",
      "high": "1386000",
      "low": "1362338.8",
      "volume": "0.03",
      "turnover": "41489.6589",
      "confirm": false,
      "timestamp": 1761670998161
    }
  ],
  "ts": 1761670998161,
  "type": "snapshot"
}
```

```json
{
  "topic": "kline.60.ETHUSDT",
  "data": [
    {
      "start": 1761667200000,
      "end": 1761670799999,
      "interval": "60",
      "open": "199999.97",
      "close": "199999.97",
      "high": "199999.97",
      "low": "199999.97",
      "volume": "1.23",
      "turnover": "245999.9631",
      "confirm": true,
      "timestamp": 1761670800201
    }
  ],
  "ts": 1761670800201,
  "type": "snapshot"
}
```

Parameters to save:

- `ts`: number. The timestamp (ms) that the system generates the data.
- `symbol`: string. Example: `ETHUSDT`.
- `start`: number. The start timestamp (ms).
- `end`: number. The end timestamp (ms).
- `open`: string. Open price.
- `close`: string. Close price.
- `high`: string. Highest price.
- `low`: string. Lowest price.
- `volume`: string. Trade volume.
- `turnover`: string. Turnover.

The `kline` data must be saved in the following tables:

- `bybit_linear_kline_15m` - to save `15m interval` data.
- `bybit_linear_kline_60m` - to save `60m interval` data.
- `bybit_linear_kline_240m` - to save `240m interval` data.
- `bybit_linear_kline_1d` - to save `1d interval` data.

Only confirmed klines must be saved.

### Bybit linear public trade data

The `public trade` data received from the Bybit websocket is the following:

```json
{
  "topic": "publicTrade.BTCUSDT",
  "type": "snapshot",
  "ts": 1760121132346,
  "data": [
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.013",
      "p": "117496.80",
      "L": "PlusTick",
      "i": "e8a1d07f-2905-5a0a-ab79-cf9ce5665d90",
      "BT": false,
      "RPI": false,
      "seq": 462754183769
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.025",
      "p": "117496.80",
      "L": "ZeroPlusTick",
      "i": "6fd1d996-dcac-5d51-888a-21407fc7bcfe",
      "BT": false,
      "RPI": false,
      "seq": 462754183769
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.042",
      "p": "117496.80",
      "L": "ZeroPlusTick",
      "i": "f21f5a6c-c99d-5720-9784-ee5ad407bf5f",
      "BT": false,
      "RPI": false,
      "seq": 462754183769
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Sell",
      "v": "0.019",
      "p": "117496.70",
      "L": "MinusTick",
      "i": "cafd7a43-6ef7-5469-b45f-d7f367efc661",
      "BT": false,
      "RPI": false,
      "seq": 462754183770
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Sell",
      "v": "0.001",
      "p": "117496.50",
      "L": "MinusTick",
      "i": "6623f371-adef-54f3-a749-110550a50765",
      "BT": false,
      "RPI": false,
      "seq": 462754183770
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Sell",
      "v": "0.001",
      "p": "117496.00",
      "L": "MinusTick",
      "i": "7ed6494e-3483-59c1-9528-cc66a0ce42e7",
      "BT": false,
      "RPI": false,
      "seq": 462754183770
    },
    {
      "T": 1760121132344,
      "s": "BTCUSDT",
      "S": "Sell",
      "v": "0.158",
      "p": "117495.50",
      "L": "MinusTick",
      "i": "c0890657-b4b6-58ab-a146-60fe15dfaf5b",
      "BT": false,
      "RPI": false,
      "seq": 462754183770
    }
  ]
}
```

NOTE: For Futures and Spot, a single message may have up to 1024 trades. As such, multiple messages may be sent for the
same `seq`.

Parameters to save:

- `T`: number. The timestamp (ms) that the order is filled.
- `s`: string. Symbol name
- `S`: string. Side of taker. **Buy**,**Sell**
- `v`: string. Trade size
- `p`: string. Trade price
- `L`: string. Direction of price change. Unique field for Perps & futures.
- `i`: string. Trade ID
- `BT`: boolean. Whether it is a block trade order or not
- `RPI`: boolean. Whether it is a RPI trade or not.
- `seq`: integer. cross sequence

The `trade` data must be saved in normalized form in the following table: `bybit_linear_public_trade`.

### Bybit linear order book data

The `order book` data received from the Bybit websocket is the following:

```json
{
  "topic": "orderbook.200.BTCUSDT",
  "type": "snapshot",
  "ts": 1761672314865,
  "data": {
    "s": "BTCUSDT",
    "b": [
      [
        "1362338.80",
        "0.962"
      ],
      [
        "1358489.80",
        "1.761"
      ],
      [
        "1355610.80",
        "0.004"
      ],
      [
        "1355045.60",
        "2.761"
      ],
      [
        "1352000.00",
        "1.532"
      ],
      [
        "1350000.00",
        "0.001"
      ],
      [
        "1349123.50",
        "1.000"
      ],
      [
        "1347527.10",
        "1.000"
      ],
      [
        "1345877.40",
        "1.000"
      ],
      [
        "1341406.20",
        "1.000"
      ],
      [
        "1332700.40",
        "0.999"
      ],
      [
        "1332000.00",
        "0.001"
      ],
      [
        "1324902.10",
        "2.000"
      ],
      [
        "1316976.40",
        "1.000"
      ],
      [
        "1314000.00",
        "0.001"
      ],
      [
        "1310503.30",
        "1.000"
      ],
      [
        "1307845.20",
        "0.002"
      ],
      [
        "1307845.10",
        "0.003"
      ],
      [
        "1307845.00",
        "0.002"
      ],
      [
        "1307844.90",
        "0.003"
      ],
      [
        "1307844.80",
        "0.003"
      ],
      [
        "1307844.70",
        "0.003"
      ],
      [
        "1307844.60",
        "0.003"
      ],
      [
        "1307844.50",
        "0.003"
      ],
      [
        "1305344.10",
        "1.000"
      ],
      [
        "1302912.90",
        "1.000"
      ],
      [
        "1298453.00",
        "0.999"
      ],
      [
        "1296968.70",
        "0.999"
      ],
      [
        "1296000.00",
        "0.001"
      ],
      [
        "1291942.30",
        "2.000"
      ],
      [
        "1289521.20",
        "1.000"
      ],
      [
        "1282079.60",
        "1.000"
      ],
      [
        "1279208.90",
        "0.999"
      ],
      [
        "1278000.00",
        "0.001"
      ],
      [
        "1277487.90",
        "1.000"
      ],
      [
        "1274619.70",
        "1.000"
      ],
      [
        "1269090.80",
        "1.000"
      ],
      [
        "1262452.10",
        "1.000"
      ],
      [
        "1260000.00",
        "0.001"
      ],
      [
        "1259683.80",
        "1.000"
      ],
      [
        "1256857.20",
        "1.000"
      ],
      [
        "1250762.10",
        "1.000"
      ],
      [
        "1248165.10",
        "0.977"
      ],
      [
        "1244704.50",
        "1.000"
      ],
      [
        "1242607.30",
        "0.999"
      ],
      [
        "1242000.00",
        "0.001"
      ],
      [
        "1241949.30",
        "1.000"
      ],
      [
        "1240156.00",
        "0.999"
      ],
      [
        "1224000.00",
        "0.001"
      ],
      [
        "1212934.30",
        "0.477"
      ],
      [
        "1211551.80",
        "0.991"
      ],
      [
        "1206000.00",
        "0.001"
      ],
      [
        "1203356.00",
        "0.697"
      ],
      [
        "1191441.60",
        "1.000"
      ],
      [
        "1188484.50",
        "0.999"
      ],
      [
        "1188000.00",
        "0.001"
      ],
      [
        "1180664.60",
        "1.000"
      ],
      [
        "1179360.40",
        "1.000"
      ],
      [
        "1177466.00",
        "1.000"
      ],
      [
        "1175237.80",
        "1.000"
      ],
      [
        "1171481.10",
        "0.999"
      ],
      [
        "1170000.00",
        "0.001"
      ],
      [
        "1169559.80",
        "1.000"
      ],
      [
        "1167145.10",
        "1.000"
      ],
      [
        "1165619.40",
        "1.000"
      ],
      [
        "1158862.10",
        "1.000"
      ],
      [
        "1157266.40",
        "4.000"
      ],
      [
        "1155514.50",
        "3.999"
      ],
      [
        "1152000.00",
        "0.001"
      ],
      [
        "1146888.50",
        "1.000"
      ],
      [
        "1135533.20",
        "0.996"
      ],
      [
        "1134000.00",
        "0.001"
      ],
      [
        "1133430.10",
        "0.995"
      ],
      [
        "1131937.50",
        "1.000"
      ],
      [
        "1121920.30",
        "0.515"
      ],
      [
        "1116000.00",
        "0.001"
      ],
      [
        "1110812.20",
        "0.997"
      ],
      [
        "1109927.80",
        "1.000"
      ],
      [
        "1107777.10",
        "1.000"
      ],
      [
        "1106528.70",
        "1.000"
      ],
      [
        "1100761.90",
        "1.000"
      ],
      [
        "1098000.00",
        "0.001"
      ],
      [
        "1090800.00",
        "1.000"
      ],
      [
        "1080000.00",
        "233765.374"
      ],
      [
        "1065960.10",
        "0.019"
      ],
      [
        "1062000.00",
        "0.001"
      ],
      [
        "1044000.00",
        "0.001"
      ],
      [
        "1043280.10",
        "0.019"
      ],
      [
        "1026800.40",
        "0.002"
      ],
      [
        "1026000.00",
        "0.001"
      ],
      [
        "1008000.00",
        "0.001"
      ],
      [
        "1000000.00",
        "0.007"
      ],
      [
        "999999.90",
        "0.001"
      ],
      [
        "997920.10",
        "0.019"
      ],
      [
        "990000.00",
        "0.001"
      ],
      [
        "972000.00",
        "0.001"
      ],
      [
        "954000.00",
        "0.001"
      ],
      [
        "936000.00",
        "0.001"
      ],
      [
        "918000.00",
        "0.001"
      ],
      [
        "900000.00",
        "0.001"
      ],
      [
        "882000.00",
        "0.001"
      ],
      [
        "864000.00",
        "0.001"
      ],
      [
        "846000.00",
        "0.001"
      ],
      [
        "828000.00",
        "0.001"
      ],
      [
        "818100.00",
        "0.001"
      ],
      [
        "810000.00",
        "0.001"
      ],
      [
        "804975.20",
        "0.001"
      ],
      [
        "800000.00",
        "0.180"
      ],
      [
        "793800.10",
        "0.019"
      ],
      [
        "792000.00",
        "0.001"
      ],
      [
        "774000.00",
        "0.001"
      ],
      [
        "756000.00",
        "0.001"
      ],
      [
        "738000.00",
        "0.001"
      ],
      [
        "720000.00",
        "0.001"
      ],
      [
        "702000.00",
        "0.001"
      ],
      [
        "684000.00",
        "0.001"
      ],
      [
        "666000.00",
        "0.001"
      ],
      [
        "648000.00",
        "0.001"
      ],
      [
        "630000.00",
        "0.001"
      ],
      [
        "612000.00",
        "0.001"
      ],
      [
        "594000.00",
        "0.001"
      ],
      [
        "576000.00",
        "0.001"
      ],
      [
        "558000.00",
        "0.001"
      ],
      [
        "545400.00",
        "0.001"
      ],
      [
        "540000.00",
        "0.001"
      ],
      [
        "522000.00",
        "0.001"
      ],
      [
        "504000.00",
        "0.001"
      ],
      [
        "486000.00",
        "0.001"
      ],
      [
        "468000.00",
        "0.001"
      ],
      [
        "453600.00",
        "0.019"
      ],
      [
        "450000.00",
        "0.001"
      ],
      [
        "432000.00",
        "0.001"
      ],
      [
        "414000.00",
        "0.001"
      ],
      [
        "396000.00",
        "0.001"
      ],
      [
        "378000.00",
        "0.001"
      ],
      [
        "362338.80",
        "0.001"
      ],
      [
        "350000.00",
        "1.000"
      ],
      [
        "300000.00",
        "1.000"
      ],
      [
        "253980.30",
        "0.001"
      ],
      [
        "242649.80",
        "0.001"
      ],
      [
        "242481.90",
        "0.001"
      ],
      [
        "240797.50",
        "0.001"
      ],
      [
        "238945.20",
        "0.001"
      ],
      [
        "237092.90",
        "0.001"
      ],
      [
        "235240.60",
        "0.001"
      ],
      [
        "233388.30",
        "0.001"
      ],
      [
        "231536.00",
        "0.001"
      ],
      [
        "230983.50",
        "0.001"
      ],
      [
        "229683.70",
        "0.001"
      ],
      [
        "227831.50",
        "0.001"
      ],
      [
        "225979.20",
        "0.001"
      ],
      [
        "224126.90",
        "0.001"
      ],
      [
        "222274.60",
        "0.001"
      ],
      [
        "220422.30",
        "0.001"
      ],
      [
        "219485.00",
        "0.001"
      ],
      [
        "218570.00",
        "0.001"
      ],
      [
        "217661.00",
        "0.161"
      ],
      [
        "216717.70",
        "0.001"
      ],
      [
        "215281.00",
        "0.162"
      ],
      [
        "215195.90",
        "0.001"
      ],
      [
        "214865.40",
        "0.001"
      ],
      [
        "214852.00",
        "0.162"
      ],
      [
        "214842.00",
        "0.001"
      ],
      [
        "213013.20",
        "0.001"
      ],
      [
        "211160.90",
        "0.001"
      ],
      [
        "209308.60",
        "0.001"
      ],
      [
        "207986.60",
        "0.001"
      ],
      [
        "207456.30",
        "0.001"
      ],
      [
        "205604.00",
        "0.001"
      ],
      [
        "203751.70",
        "0.001"
      ],
      [
        "201899.40",
        "0.001"
      ],
      [
        "200047.10",
        "0.001"
      ],
      [
        "200000.00",
        "1.000"
      ],
      [
        "198194.90",
        "0.001"
      ],
      [
        "196488.20",
        "0.001"
      ],
      [
        "196342.60",
        "0.001"
      ],
      [
        "194490.30",
        "0.001"
      ],
      [
        "192638.00",
        "0.001"
      ],
      [
        "192408.90",
        "0.001"
      ],
      [
        "190785.70",
        "0.001"
      ],
      [
        "190227.30",
        "0.001"
      ],
      [
        "188933.40",
        "0.001"
      ],
      [
        "188775.20",
        "0.001"
      ],
      [
        "187323.10",
        "0.001"
      ],
      [
        "187081.10",
        "0.001"
      ],
      [
        "185871.00",
        "0.001"
      ],
      [
        "185228.80",
        "0.001"
      ],
      [
        "184989.80",
        "0.001"
      ],
      [
        "184818.00",
        "0.001"
      ],
      [
        "184418.90",
        "0.001"
      ],
      [
        "183376.50",
        "0.001"
      ],
      [
        "182966.80",
        "0.001"
      ],
      [
        "181524.30",
        "0.001"
      ],
      [
        "181514.60",
        "0.001"
      ],
      [
        "180062.50",
        "0.001"
      ],
      [
        "179672.00",
        "0.001"
      ],
      [
        "178610.40",
        "0.001"
      ],
      [
        "177819.70",
        "0.001"
      ],
      [
        "177227.10",
        "0.001"
      ],
      [
        "177158.30",
        "0.001"
      ]
    ],
    "a": [
      [
        "1386000.00",
        "0.001"
      ],
      [
        "1404000.00",
        "0.001"
      ],
      [
        "1422000.00",
        "0.001"
      ],
      [
        "1520529.20",
        "0.001"
      ],
      [
        "1999999.00",
        "5.000"
      ]
    ],
    "u": 17424,
    "seq": 9538008553
  },
  "cts": 1761672311767
}
```

Parameters to save:

- `s`: string. Symbol name.
- `b`: array. Bids. For snapshot stream. Sorted by price in descending order
- `b[0]`: string. Bid price.
- `b[1]`: string. Bid size. The delta data has size=0, which means that all quotations for this price have been filled
  or cancelled.
- `a`: array Asks. For snapshot stream. Sorted by price in ascending order.
- `a[0]`: string. Ask price.
- `a[1]`: string. Ask size. The delta data has size=0, which means that all quotations for this price have been filled
  or cancelled.
- `u`: integer. Update ID. Occasionally, you'll receive "u"=1, which is a snapshot data due to the restart of the
  service. So please overwrite your local orderbook. For level 1 of linear, inverse Perps and Futures, the snapshot
  data will be pushed again when there is no change in 3 seconds, and the "u" will be the same as that in the previous
  message.
- `seq`: integer. Cross sequence. You can use this field to compare different levels orderbook data, and for the smaller
  seq, then it means the data is generated earlier.
- `cts`: number. The timestamp from the matching engine when this orderbook data is produced. It can be correlated with
  `T` from public trade channel.

The `order book` data must be saved in normalized form in the following table: `bybit_linear_order_book_1`,
`bybit_linear_order_book_50`, `bybit_linear_order_book_200`, `bybit_linear_order_book_1000`.

### Bybit linear all liquidation data

The `all liquidation` data received from the Bybit websocket is the following:

```json
{
  "topic": "allLiquidation.ROSEUSDT",
  "type": "snapshot",
  "ts": 1739502303204,
  "data": [
    {
      "T": 1739502302929,
      "s": "ROSEUSDT",
      "S": "Sell",
      "v": "20000",
      "p": "0.04499"
    }
  ]
}
```

Parameters to save:

- `T`: number. The updated timestamp (ms).
- `s`: string. Symbol name.
- `S`: string. Position side. Buy,Sell. When you receive a Buy update, this means that a long position has been
  liquidated.
- `v`: string. Executed size.
- `p`: string. Bankruptcy price.

The `all liquidation` data must be saved in normalized form in the following table: `bybit_linear_all_liquidation`.