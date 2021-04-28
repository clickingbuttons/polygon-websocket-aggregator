# polygon-websocket-aggregator

Aggregates trades for a ticker into candlesticks using [polygon.io's websocket feed.](https://polygon.io/docs/websockets/getting-started) Updates candlesticks for late trades up to a certain buffer size.

## Usage
```sh
export POLYGON_KEY=
go run cmd/polygon-websocket-aggregator/main.go <ticker>
```

Set `var DEBUG = false` in `cmd/polygon-websocket-aggregator/main.go` to view the ring as it updates.
