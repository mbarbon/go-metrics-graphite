package graphite

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func floatEquals(a, b float64) bool {
	return (a-b) < 0.000001 && (b-a) < 0.000001
}

func ExampleGraphite() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go Graphite(metrics.DefaultRegistry, 1*time.Second, "some.prefix", addr)
}

func ExampleWithConfig() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go WithConfig(Config{
		Addr:          addr,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: 1 * time.Second,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
	})
}

func NewTestServer(t *testing.T, prefix string) (map[string]float64, net.Listener, metrics.Registry, Config, *sync.WaitGroup, *int32) {
	res := make(map[string]float64)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	var wg sync.WaitGroup
	var closed int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if atomic.LoadInt32(&closed) == 1 {
					break
				} else {
					t.Fatal("dummy server error:", err)
				}
			}
			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')
			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseFloat(parts[1], 0)
				if testing.Verbose() {
					t.Log("recv", parts[0], i)
				}
				res[parts[0]] = res[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			wg.Done()
			conn.Close()
		}
	}()

	r := metrics.NewRegistry()

	c := Config{
		Addr:          ln.Addr().(*net.TCPAddr),
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        prefix,
	}

	return res, ln, r, c, &wg, &closed
}

func TestWrites(t *testing.T) {
	res, l, r, c, wg, closed := NewTestServer(t, "foobar")
	defer l.Close()
	defer atomic.StoreInt32(closed, 1)

	metrics.GetOrRegisterCounter("foo", r).Inc(2)

	// TODO: Use a mock meter rather than wasting 10s to get a QPS.
	for i := 0; i < 10*4; i++ {
		metrics.GetOrRegisterMeter("bar", r).Mark(1)
		time.Sleep(200 * time.Millisecond)
	}

	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 5)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 4)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 3)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 2)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 1)

	wg.Add(1)
	Once(c)
	wg.Wait()

	if expected, found := 2.0, res["foobar.foo.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 40.0, res["foobar.bar.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5.0, res["foobar.bar.one-minute"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5.0, res["foobar.baz.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5000.0, res["foobar.baz.99-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 3000.0, res["foobar.baz.50-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}
}
