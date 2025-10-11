# Issue 6: Define `bybit_linear_all_liquidation` table

In this `crypto-scout-collector-db` project we are going to define a new `bybit_linear_all_liquidation` table.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Do not hallucinate.

## Tasks

- As the expert database engineer review the current `init.sql` script implementation in `crypto-scout-collector-db` project and
  update it by defining the `bybit_linear_all_liquidation` table.
- As the expert database engineer define for the `bybit_linear_all_liquidation` table indexes, retentions and
  compressions.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the expert database engineer Rely on the sample of the data section.
- As the technical writer update the `README.md` and `timescaledb-production-setup.md` files with your results.
- As the technical writer update the `issue-6-define-bybit-linear-all-liquidation-table.md` file with your resolution.

## Sample of the data

```json
{
  "ts": 1760108679985,
  "data": [
    {
      "T": 1760108679532,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.032",
      "p": "119261.10"
    },
    {
      "T": 1760108679542,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.014",
      "p": "119253.30"
    },
    {
      "T": 1760108679584,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.002",
      "p": "119261.50"
    },
    {
      "T": 1760108679863,
      "s": "BTCUSDT",
      "S": "Buy",
      "v": "0.123",
      "p": "119252.90"
    }
  ]
}
```

### Field mapping from sample JSON

- `ts`: number. The timestamp (ms) that the system generates the data
- `data`: Object.
- `T`: number. The updated timestamp (ms)
- `s`: string. Symbol name
- `S`: string. Position side. Buy,Sell. When you receive a Buy update, this means that a long position has been
  liquidated
- `v`: string. Executed size
- `p`: string. Bankruptcy price