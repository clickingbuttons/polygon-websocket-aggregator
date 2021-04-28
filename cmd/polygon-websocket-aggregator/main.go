package main

import (
	"container/ring"
	"fmt"
	"os"
	"time"

	"golang.org/x/net/websocket"
)

var DEBUG = false // sets small buffer size, spoofs late trade messages, shows ring
var agg_period = 30 * time.Second
var buffer = 1 * time.Hour // must be greater than agg_period
var buff_size int
var candlesticks *ring.Ring    // ring of buf_size candlesticks. shifts up when full. if trade arrives before ring minimum it's ignored
var cur_candlestick *ring.Ring // next candlestick to be printed

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
	bucket int64 // millis
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
		errorString := fmt.Sprintf("Expected status %s but got %s", expected_status, msg[0].Status)
		panic(errorString)
	} else {
		fmt.Println(expected_status)
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

func get_time_bucket(ts int64) int64 {
	return ts - ts%agg_period.Milliseconds()
}

func get_candlestick(bucket int64) *ring.Ring {
	r := candlesticks
	for i := 0; i < r.Len(); i++ {
		if r.Value == nil || r.Value.(OHLCV).bucket == bucket {
			return r
		}
		r = r.Next()
	}

	// Set oldest element in ring to nil and return it
	candlesticks = candlesticks.Move(1)
	r = candlesticks.Prev()
	r.Value = nil
	return r
}

func aggregate(c chan trade_message, start_at time.Time) {
	for trade := range c {
		if trade.T < start_at.UnixNano()/1_000_000 {
			// Ignore first partial bar
			if DEBUG {
				fmt.Println("ignoring trade", trade.T, "before", start_at.Unix()*1_000)
			}
			continue
		}
		bucket := get_time_bucket(trade.T)
		existing_candlestick := get_candlestick(bucket)
		var candlestick OHLCV
		if existing_candlestick.Value == nil {
			candlestick = OHLCV{bucket: bucket}
		} else {
			candlestick = existing_candlestick.Value.(OHLCV)
		}

		if candlestick.open == 0 {
			candlestick.open = trade.P
			candlestick.high = trade.P
			candlestick.low = trade.P
		} else if candlestick.high < trade.P {
			candlestick.high = trade.P
		} else if candlestick.low > trade.P {
			candlestick.low = trade.P
		}
		candlestick.close = trade.P
		candlestick.volume += trade.S

		if cur_candlestick.Prev().Value != nil && bucket <= cur_candlestick.Prev().Value.(OHLCV).bucket {
			fmt.Print("(late) ")
			// Outside of buffer range
			if existing_candlestick.Value == nil {
				print_time_millis(trade.T)
				fmt.Println(" - ignored")
				continue
			} else {
				print_ohlcv(candlestick)
			}
		}
		existing_candlestick.Value = candlestick
		if DEBUG {
			candlesticks.Do(func(p interface{}) {
				fmt.Println(p)
			})
			fmt.Println()
		}
	}
}

func print_time(t time.Time) {
	fmt.Printf(
		"%02d:%02d:%02d",
		t.Hour(),
		t.Minute(),
		t.Second(),
	)
}

func print_time_millis(millis int64) {
	t := time.Unix(millis/1000, millis%1000)
	print_time(t)
}

func print_ohlcv(candlestick OHLCV) {
	print_time_millis(candlestick.bucket)
	fmt.Printf(
		" - open $%0.2f, close $%0.2f, high $%0.2f, low $%0.2f, volume %d\n",
		candlestick.open,
		candlestick.close,
		candlestick.high,
		candlestick.low,
		candlestick.volume,
	)
}

func print_aggs() {
	timer := time.NewTicker(agg_period)
	for {
		tick := (<-timer.C).Add(-agg_period)
		candlestick := cur_candlestick.Value

		if candlestick != nil {
			print_ohlcv(candlestick.(OHLCV))
		} else {
			print_time(tick)
			fmt.Println(" - no data")
		}
		cur_candlestick = cur_candlestick.Next()
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <ticker>\n", os.Args[0])
		os.Exit(1)
	}
	ticker := os.Args[1]
	buff_size = int(buffer.Nanoseconds()/agg_period.Nanoseconds()) + 1
	if DEBUG {
		buff_size = 3
	}
	candlesticks = ring.New(buff_size)
	cur_candlestick = candlesticks

	wss := open_wss(ticker)
	defer wss.Close()

	// Snooze until nearest 5s interval starts.
	now := time.Now()
	start_at := now.Round(agg_period)
	to_sleep := start_at.Sub(now)
	if to_sleep.Microseconds() < 0 {
		to_sleep = agg_period + to_sleep
		start_at = start_at.Add(agg_period)
	}
	fmt.Printf(
		"Waiting %s to start at %s interval %02d:%02d:%02d\n",
		to_sleep.String(),
		agg_period.String(),
		start_at.Hour(),
		start_at.Minute(),
		start_at.Second(),
	)
	time.Sleep(to_sleep)

	trade_messages := make(chan trade_message, 1000)

	go aggregate(trade_messages, start_at)
	go print_aggs()
	if DEBUG {
		// Spoof 2 late trade messages
		go func() {
			message := trade_message{P: 200, S: 100, T: int64(start_at.UnixNano() / 1_000_000)}
			// This one should print a new bar
			time.Sleep(5 / 2 * agg_period)
			trade_messages <- message
			// This one should be ignored
			time.Sleep(5 / 2 * agg_period)
			trade_messages <- message
		}()
	}

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
