# Issue 4: Define `bybit_spot_public_trade` table

In this `crypto-scout-collector-db` project we are going to define a new `bybit_spot_public_trade` table.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Do not hallucinate.
- Use the following technical stack: `java 25`

## Tasks

- As the expert database engineer review the current `init.sql` script implementation in `crypto-scout-collector-db` project and
  update it by defining the `bybit_spot_public_trade` table.
- As the expert database engineer define for the `bybit_spot_public_trade` table indexes, retentions and compressions.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the expert database engineer Rely on the sample of the data section.
- As the technical writer update the `README.md` and `timescaledb-production-setup.md` files with your results.
- As the technical writer update the `4-define-bybit-spot-public-trade-table.md` file with your resolution.

## Sample of the data

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

## Resolution

- **[change]** Added hypertable `crypto_scout.bybit_spot_public_trade` in `script/init.sql` with schema, indexes,
  compression, reorder, and retention.

### Table schema: `crypto_scout.bybit_spot_public_trade`

- **Columns**
    - `id BIGSERIAL`
    - `timestamp TIMESTAMPTZ NOT NULL`
    - `trade_time TIMESTAMPTZ NOT NULL`
    - `symbol TEXT NOT NULL`
    - `taker_side TEXT NOT NULL`
    - `size NUMERIC(20, 8) NOT NULL`
    - `price NUMERIC(20, 8) NOT NULL`
    - `trade_id TEXT NOT NULL`
    - `block_trade BOOLEAN NOT NULL`
    - `rpi BOOLEAN`
    - `cross_sequence BIGINT NOT NULL`
- **Primary key**: `(id, trade_time)`
- **Hypertable**: partitioned by `trade_time` (1-day chunks)

### Indexes

- `idx_bybit_spot_public_trade_trade_time` on `(trade_time DESC)`
- `idx_bybit_spot_public_trade_symbol_trade_time` on `(symbol, trade_time DESC)`
- Reorder: `add_reorder_policy('crypto_scout.bybit_spot_public_trade', 'idx_bybit_spot_public_trade_trade_time')`

### Compression

- `ALTER TABLE crypto_scout.bybit_spot_public_trade SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol',
  timescaledb.compress_orderby = 'trade_time DESC, id DESC'
);`
- Policy: `add_compression_policy('crypto_scout.bybit_spot_public_trade', INTERVAL '7 days')`

### Retention

- Policy: `add_retention_policy('crypto_scout.bybit_spot_public_trade', INTERVAL '90 days')`

### Field mapping from sample JSON to schema

- `ts` (ms) → `timestamp` (epoch millis → timestamptz)
- `data[].T` (ms) → `trade_time` (epoch millis → timestamptz)
- `data[].s` → `symbol`
- `data[].S` → `taker_side`
- `data[].v` → `size`
- `data[].p` → `price`
- `data[].i` → `trade_id`
- `data[].BT` → `block_trade`
- `data[].RPI` → `rpi`
- `data[].seq` → `cross_sequence`

Notes:

- Options-only fields `mP`, `iP`, `mIv`, `iv` are intentionally omitted for spot storage.
- A single message may batch up to 1024 trades; order by `trade_time` and `trade_id` for deterministic processing.