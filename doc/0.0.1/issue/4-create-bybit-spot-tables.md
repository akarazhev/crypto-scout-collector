# Issue 4: Create bybit spot tables

In this `crypto-scout-collector-db` project we are going to create the following bybit spot tables to save the data
received from the Bybit websocket: `bybit_spot_kline_15m`, `bybit_spot_kline_60m`, `bybit_spot_kline_240m`,
`bybit_spot_kline_1d`, `bybit_spot_public_trade`, `bybit_spot_order_book_200`. Table schemas must be optimal for
the analysis, inserting data. Retentions and compressions must be defined.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Use the following technical stack: `timescale/timescaledb:latest-pg17`.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` review the current `init.sql` script implementation in `crypto-scout-collector-db`
  project and update it by defining the following tables: `bybit_spot_kline_15m`, `bybit_spot_kline_60m`,
  `bybit_spot_kline_240m`, `bybit_spot_kline_1d`, `bybit_spot_public_trade`, `bybit_spot_order_book_200`.
- As the `expert database engineer` define for tables indexes, retentions and compressions. Table schemas must be
  optimal for the analysis, inserting data. Retentions and compressions must be defined.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the `expert database engineer` rely on the sample of the data section.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `4-create-bybit-spot-tables.md` file with your resolution.

## Sample of the data

### Bybit spot klines

```json
{
  "type": "snapshot",
  "topic": "kline.15.ETHUSDT",
  "data": [
    {
      "start": 1761591600000,
      "end": 1761592499999,
      "interval": "15",
      "open": "4951.21",
      "close": "4951.21",
      "high": "4951.21",
      "low": "4951.21",
      "volume": "0",
      "turnover": "0",
      "confirm": false,
      "timestamp": 1761591715966
    }
  ],
  "ts": 1761591715966
}
```

Parameters to save:

- `ts`: number. The timestamp (ms) that the system generates the data.
- `symbol`: string. Example: `ETHUSDT`.
- `start`: number. The start timestamp (ms).
- `end`: number. The end timestamp (ms).
- `interval`: string. Kline interval.
- `open`: string. Open price.
- `close`: string. Close price.
- `high`: string. Highest price.
- `low`: string. Lowest price.
- `volume`: string. Trade volume.
- `turnover`: string. Turnover.

### Bybit spot public trade

```json
{
  "ts": 1760117214053,
  "data": [
    {
      "i": "2290000000908760214",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.000061",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760215",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.084697",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760216",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.1069",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760217",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.050819",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760218",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.105026",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760219",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.07472",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760220",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.029913",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760221",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.03167",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760222",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.0256",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760223",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.001",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760224",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.000212",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760225",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.000212",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760226",
      "T": 1760117214051,
      "p": "118067.5",
      "v": "0.001",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760227",
      "T": 1760117214051,
      "p": "118066.4",
      "v": "0.01",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760228",
      "T": 1760117214051,
      "p": "118066",
      "v": "0.016938",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760229",
      "T": 1760117214051,
      "p": "118065.9",
      "v": "0.004233",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    },
    {
      "i": "2290000000908760230",
      "T": 1760117214051,
      "p": "118065.5",
      "v": "0.042544",
      "S": "Sell",
      "seq": 87416325577,
      "s": "BTCUSDT",
      "BT": false,
      "RPI": false
    }
  ]
}
```

NOTE: For Futures and Spot, a single message may have up to 1024 trades. As such, multiple messages may be sent for the
same `seq`.

- `ts`: number. The timestamp (ms) that the system generates the data
- `data`: array. Object. Sorted by the time the trade was matched in ascending order:
- `i`: string. Trade ID
- `T`: number. The timestamp (ms) that the order is filled
- `p`: string. Trade price
- `v`: string. Trade size
- `S`: string. Side of taker. **Buy**,**Sell**
- `seq`: integer. cross sequence
- `s`: string. Symbol name
- `BT`: boolean. Whether it is a block trade order or not
- `RPI`: boolean. Whether it is a RPI trade or not