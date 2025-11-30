# Issue 22: Implement collecting `btc-usd` data for the `1w` timeframe that is provided by `CoinMarketCap`

In this `crypto-scout-collector` project let's implement repository and service functions. The related tests should 
also be updated. The `SQL` table with read and save has been already implemented. You can rely on the same 
implementation by for the `1d` timeframe.

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.

## Conditions

- Rely on the same implementation by for the `1d` timeframe.
- Rely on section: `BTC-USD` 1W data model in json.
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `Java 25`, `ActiveJ 6.0-rc2`, `amqp-client 5.26.0`,
  `stream-client 1.2.0`.
- Follow the current code style.
- Do not hallucinate.

## Tasks

- Rely on the same implementation by for the `1d` timeframe.
- As the `expert java engineer` update the implementations to reflect changes.
- As the `expert java engineer` update the implementations to test changes.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.

## `BTC-USD` 1D data model in json

```json
{
  "id": 1,
  "name": "Bitcoin",
  "symbol": "BTC",
  "timeEnd": "1522627199",
  "quotes": [
    {
      "timeOpen": "2025-11-17T00:00:00.000Z",
      "timeClose": "2025-11-23T23:59:59.999Z",
      "timeHigh": "2025-11-17T08:26:00.000Z",
      "timeLow": "2025-11-21T12:30:00.000Z",
      "quote": {
        "name": "2781",
        "open": 94180.8763285011,
        "high": 95928.3695793783,
        "low": 80659.8124264662,
        "close": 86805.0080755520,
        "volume": 659958211589.4600000000,
        "marketCap": 1731864376001.4300000000,
        "circulatingSupply": 19952637,
        "timestamp": "2025-11-23T23:59:59.999Z"
      }
    }
  ]
}
```