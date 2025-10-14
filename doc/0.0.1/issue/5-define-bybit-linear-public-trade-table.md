# Issue 5: Define `bybit_linear_public_trade` table

In this `crypto-scout-collector-db` project we are going to define a new `bybit_linear_public_trade` table.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Rely on the `bybit_spot_public_trade` table implementation as an sample.
- Do not hallucinate.

## Tasks

- As the expert database engineer review the current `init.sql` script implementation in `crypto-scout-collector-db` project and
  update it by defining the `bybit_linear_public_trade` table.
- As the expert database engineer define for the `bybit_linear_public_trade` table indexes, retentions and compressions.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the expert database engineer Rely on the sample of the data section.
- As the technical writer update the `README.md` and `timescaledb-production-setup.md` files with your results.
- As the technical writer update the `issue-4-define-bybit-linear-public-trade-table.md` file with your resolution.

## Sample of the data

```json
{
  "ts": 1672304486868,
  "data": [
    {
      "T": 1672304486865,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.001",
      "p": "16578.50",
      "L": "PlusTick",
      "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
      "BT": false,
      "seq": 1783284617
    }
  ]
}
```

NOTE: For Futures and Spot, a single message may have up to 1024 trades. As such, multiple messages may be sent for the
same `seq`.

- `ts`: number. The timestamp (ms) that the system generates the data
- `data`: array. Object. Sorted by the time the trade was matched in ascending order
- `T`: number. The timestamp (ms) that the order is filled
- `s`: string. Symbol name
- `S`: string. Side of taker. **Buy**,**Sell**
- `v`: string. Trade size
- `p`: string. Trade price
- `L`: string. Direction of price change. Unique field for Perps & futures
- `i`: string. Trade ID
- `BT`: boolean. Whether it is a block trade order or not
- `RPI`: boolean. Whether it is a RPI trade or not
- `seq`: integer. cross sequence.
