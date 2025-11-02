# Issue 11: Create bybit spot kline 1m and 5m tables

In this `crypto-scout-collector-db` project we are going to create the following bybit spot tables to save the data
received from the Bybit websocket: `bybit_spot_kline_1m`, `bybit_spot_kline_5m`. Table schemas must be in normalized form
and optimal to perform analysis and efficient saving the data. Data removal and compression must be done with:
`retention` and `compression` policies.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Use the following technical stack: `timescale/timescaledb:latest-pg17`.
- Use the same data schemas for `kline` data.
- Use human-readable names for parameter names.
- Normalize data schemas for optimal savings and analysts.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` review the current `init.sql` script implementation in `crypto-scout-collector-db`
  project and update it by defining the following tables: `bybit_spot_kline_1m`, `bybit_spot_kline_5m`.
- As the `expert database engineer` define for tables indexes, retentions and compressions. Table schemas must be in
  normalized form and optimal to perform analysis and efficient saving the data. Data removal and compression must be
  done with: `retention` and `compression` policies.
- As the `expert database engineer` recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the `expert database engineer` rely on the definition of the data section.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `11-create-bybit-spot-kline-1m-5m-tables.md` file with your resolution.

## Definition of the data

### Bybit spot klines data

The `kline` data received from the Bybit websocket is the following:

```json
{
  "type": "snapshot",
  "topic": "kline.5.ETHUSDT",
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

```json
{
  "type": "snapshot",
  "topic": "kline.5.BTCUSDT",
  "data": [
    {
      "start": 1761634800000,
      "end": 1761638399999,
      "interval": "60",
      "open": "115488.04",
      "close": "115116",
      "high": "115488.04",
      "low": "114929.92",
      "volume": "0.435693",
      "turnover": "50076.34804012",
      "confirm": false,
      "timestamp": 1761637002902
    }
  ],
  "ts": 1761637002902
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

- `bybit_spot_kline_1m` - to save `1m interval` data.
- `bybit_spot_kline_5m` - to save `5m interval` data.

Only confirmed klines must be saved.