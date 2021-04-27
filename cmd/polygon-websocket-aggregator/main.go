package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/net/websocket"
)

type to_message struct {
	Action string
	Params string
}

type connect_message struct {
	Ev      string
	Status  string
	Message string
}

type trade_message struct {
	P float64
	S uint64
	C []uint64
	T int64 // millis
}

type OHLCV struct {
	nanos  int64
	open   float64
	high   float64
	low    float64
	close  float64
	volume uint64
}

func expect_response(to_msg *to_message, expected_status string, wss *websocket.Conn) {
	if to_msg != nil {
		err := websocket.JSON.Send(wss, to_msg)
		if err != nil {
			panic(err)
		}
	}

	msg := make([]connect_message, 1)
	err := websocket.JSON.Receive(wss, &msg)
	if err != nil {
		panic(err)
	}
	if msg[0].Status != expected_status {
		errorString := fmt.Sprintf("Expected response %s to have status %s", msg, expected_status)
		panic(errorString)
	}
}

func open_wss(ticker string) *websocket.Conn {
	POLYGON_KEY, ok := os.LookupEnv("POLYGON_KEY")
	if !ok {
		fmt.Printf("%s requires environment var POLYGON_KEY\n", os.Args[0])
		os.Exit(1)
	}

	wss, err := websocket.Dial("wss://socket.polygon.io/stocks", "", "http://localhost/")
	if err != nil {
		panic(err)
	}

	expect_response(nil, "connected", wss)
	expect_response(&to_message{Action: "auth", Params: POLYGON_KEY}, "auth_success", wss)
	expect_response(&to_message{Action: "subscribe", Params: fmt.Sprintf("T.%s", ticker)}, "success", wss)

	return wss
}

func get_time_bucket(ts int64, agg_period time.Duration) int64 {
	return ts - ts%agg_period.Milliseconds()
}

func get_candlesticks_index(candlesticks []OHLCV, bucket int64) int {
	idx := -1
	for i, c := range candlesticks {
		if c.nanos == bucket || c.nanos == 0 {
			idx = i
			break
		}
	}

	return idx
}

func aggregate(candlesticks []OHLCV, c chan trade_message, agg_period time.Duration) {
	for trade := range c {
		bucket := get_time_bucket(trade.T, agg_period)
		idx := get_candlesticks_index(candlesticks, bucket)
		if idx == -1 {
			// TODO: delete oldest value
			idx = len(candlesticks)
		}
		candlesticks[idx].nanos = bucket

		if candlesticks[idx].open == 0 {
			candlesticks[idx].open = trade.P
			candlesticks[idx].high = trade.P
			candlesticks[idx].low = trade.P
		} else if candlesticks[idx].high < trade.P {
			candlesticks[idx].high = trade.P
		} else if candlesticks[idx].low > trade.P {
			candlesticks[idx].low = trade.P
		}
		candlesticks[idx].close = trade.P
		candlesticks[idx].volume += trade.S

		// fmt.Println(trade, idx, candlesticks[0])
	}
}

func print_aggs(candlesticks []OHLCV, agg_period time.Duration) {
	timer := time.NewTicker(agg_period)
	for {
		tick := (<-timer.C).Add(-agg_period)
		bucket := get_time_bucket(int64(tick.UTC().Nanosecond()/1000), agg_period)
		idx := get_candlesticks_index(candlesticks, bucket) - 1
		candlestick := candlesticks[idx]
		fmt.Printf(
			"%02d:%02d:%02d - open $%0.2f, close $%0.2f, high $%0.2f, low $%0.2f, volume %d\n",
			tick.Hour(),
			tick.Minute(),
			tick.Second(),
			candlestick.open,
			candlestick.close,
			candlestick.high,
			candlestick.low,
			candlestick.volume,
		)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <ticker>\n", os.Args[0])
		os.Exit(1)
	}
	ticker := os.Args[1]

	wss := open_wss(ticker)
	defer wss.Close()

	agg_period := 5 * time.Second
	trade_messages := make(chan trade_message, 1000)
	candlesticks := make([]OHLCV, int(60*60/agg_period.Seconds())+1) // allow 1h buffer

	// Snooze until nearest 5s interval starts.
	now := time.Now()
	start_at := now.Round(agg_period)
	to_sleep := start_at.Sub(now)
	if to_sleep.Microseconds() < 0 {
		to_sleep = agg_period + to_sleep
	}
	fmt.Println(now, now.Round(agg_period))
	fmt.Printf(
		"Sleeping %s to start at %s interval %02d:%02d:%02d\n",
		to_sleep.String(),
		agg_period.String(),
		start_at.Hour(),
		start_at.Minute(),
		start_at.Second(),
	)
	time.Sleep(to_sleep)
	go aggregate(candlesticks, trade_messages, agg_period)
	go print_aggs(candlesticks, agg_period)

	for {
		var msg = make([]trade_message, 1000)
		// err := websocket.Message.Receive(wss, &msg)
		// fmt.Println("Got message", string(msg))
		err := websocket.JSON.Receive(wss, &msg)
		if err != nil {
			panic(err)
		}
		for _, m := range msg {
			trade_messages <- m
		}
	}
}
