// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goschedstats

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/stretchr/testify/require"
)

func TestNumRunnableGoroutines(t *testing.T) {
	// Start 500 goroutines that never finish.
	const n = 400
	for i := 0; i < n; i++ {
		go func(i int) {
			a := 1
			for x := 0; x >= 0; x++ {
				a = a*13 + x
			}
		}(i)
	}
	// When we run, we expect at most GOMAXPROCS-1 of the n goroutines to be
	// running, with the rest waiting.
	expected := n - runtime.GOMAXPROCS(0) + 1
	testutils.SucceedsSoon(t, func() error {
		if n, _ := numRunnableGoroutines(); n < expected {
			return fmt.Errorf("only %d runnable goroutines, expected %d", n, expected)
		}
		return nil
	})
}

type testTimeTicker struct {
	numResets         int
	lastResetDuration time.Duration
}

func (t *testTimeTicker) Reset(d time.Duration) {
	t.numResets++
	t.lastResetDuration = d
}

func TestSchedStatsTicker(t *testing.T) {
	runnable := 0
	numRunnable := func() (numRunnable int, numProcs int) {
		return runnable, 1
	}
	var callbackSamplePeriod time.Duration
	var numCallbacks int
	cb := func(numRunnable int, numProcs int, samplePeriod time.Duration) {
		require.Equal(t, runnable, numRunnable)
		require.Equal(t, 1, numProcs)
		callbackSamplePeriod = samplePeriod
		numCallbacks++
	}
	cbs := []callbackWithID{{cb, 0}}
	now := timeutil.UnixEpoch
	startTime := now
	sst := schedStatsTicker{
		lastTime:              now,
		curPeriod:             samplePeriodShort,
		numRunnableGoroutines: numRunnable,
	}
	tt := testTimeTicker{}
	// Tick every 1ms until the reportingPeriod has elapsed.
	for i := 1; ; i++ {
		now = now.Add(samplePeriodShort)
		sst.getStatsOnTick(now, cbs, &tt)
		if now.Sub(startTime) <= reportingPeriod {
			// No reset of the time ticker.
			require.Equal(t, 0, tt.numResets)
			// Each tick causes a callback.
			require.Equal(t, i, numCallbacks)
			require.Equal(t, samplePeriodShort, callbackSamplePeriod)
		} else {
			break
		}
	}
	// Since underloaded, the time ticker is reset to samplePeriodLong, and this
	// period is provided to the latest callback.
	require.Equal(t, 1, tt.numResets)
	require.Equal(t, samplePeriodLong, tt.lastResetDuration)
	require.Equal(t, samplePeriodLong, callbackSamplePeriod)
	// Increase load so no longer underloaded.
	runnable = 2
	startTime = now
	tt.numResets = 0
	for i := 1; ; i++ {
		now = now.Add(samplePeriodLong)
		sst.getStatsOnTick(now, cbs, &tt)
		if now.Sub(startTime) <= reportingPeriod {
			// No reset of the time ticker.
			require.Equal(t, 0, tt.numResets)
			// Each tick causes a callback.
			require.Equal(t, samplePeriodLong, callbackSamplePeriod)
		} else {
			break
		}
	}
	// No longer underloaded, so the time ticker is reset to samplePeriodShort,
	// and this period is provided to the latest callback.
	require.Equal(t, 1, tt.numResets)
	require.Equal(t, samplePeriodShort, tt.lastResetDuration)
	require.Equal(t, samplePeriodShort, callbackSamplePeriod)
}

func BenchmarkNumRunnableGoroutines(b *testing.B) {
	var numRunnable, numProcs int
	for i := 0; i < b.N; i++ {
		numRunnable, numProcs = numRunnableGoroutines()
	}
	b.StopTimer()
	fmt.Printf("r: %d, p: %d\n", numRunnable, numProcs)
}

/*
MacOS
Histogram
[0s,16.383µs): 9710
[16.384µs,32.767µs): 388640
[32.768µs,49.151µs): 1487
[49.152µs,65.535µs): 141
[65.536µs,81.919µs): 14
[81.92µs,98.303µs): 4
[98.304µs,114.687µs): 0
[114.688µs,131.071µs): 0
[131.072µs,147.455µs): 0
[147.456µs,163.839µs): 1
BenchmarkNumRunnableGoroutinesPlusTicker-10    	  400000	     20055 ns/op	       0 B/op	       0 allocs/op

Linux gceworker is terrible
BenchmarkNumRunnableGoroutinesPlusTicker-16    	    6849	    177815 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkNumRunnableGoroutinesPlusTicker(b *testing.B) {
	// var numRunnable, numProcs int
	var lastTime time.Time
	i := 0
	tickerDuration := 20 * time.Microsecond
	ticker := time.NewTicker(tickerDuration)
	durations := hdrhistogram.New(int64(tickerDuration), int64(1000*tickerDuration), 3)
	b.ResetTimer()
	for {
		now := <-ticker.C
		if i > 0 {
			dur := now.Sub(lastTime)
			durations.RecordValue(int64(dur))
		}
		lastTime = now
		_, _ = numRunnableGoroutines()
		i++
		if i == b.N {
			break
		}
	}
	b.StopTimer()
	bars := durations.Distribution()
	fmt.Printf("\nHistogram\n")
	for i := range bars {
		fmt.Printf("[%s,%s): %d\n", time.Duration(bars[i].From).String(),
			time.Duration(bars[i].To), bars[i].Count)
	}
}

/*
Histogram
[16.384µs,32.767µs): 397939
[32.768µs,49.151µs): 1723
[49.152µs,65.535µs): 248
[65.536µs,81.919µs): 73
[81.92µs,98.303µs): 8
[98.304µs,114.687µs): 6
[180.224µs,196.607µs): 1
[1.015808ms,1.032191ms): 1
BenchmarkNumRunnableGoroutinesPlusSleep-10    	  400000	     25442 ns/op	       0 B/op	       0 allocs/op

Linux gceworker is terrible
BenchmarkNumRunnableGoroutinesPlusSleep-16    	    2931	    533982 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkNumRunnableGoroutinesPlusSleep(b *testing.B) {
	var lastTime time.Time
	i := 0
	tickerDuration := 20 * time.Microsecond
	durations := hdrhistogram.New(int64(tickerDuration), int64(1000*tickerDuration), 3)
	b.ResetTimer()
	for {
		now := timeutil.Now()
		if i > 0 {
			dur := now.Sub(lastTime)
			durations.RecordValue(int64(dur))
		}
		lastTime = now
		_, _ = numRunnableGoroutines()
		i++
		if i == b.N {
			break
		}
		time.Sleep(tickerDuration)
	}
	b.StopTimer()
	bars := durations.Distribution()
	fmt.Printf("\nHistogram\n")
	for i := range bars {
		if bars[i].Count == 0 {
			continue
		}
		fmt.Printf("[%s,%s): %d\n", time.Duration(bars[i].From).String(),
			time.Duration(bars[i].To), bars[i].Count)
	}
}

/*
See

https://github.com/golang/go/issues/27707#issuecomment-470824218: "We still
handle timers on a separate thread, so there are like 4 excessive thread
context switches per timer. Timers need to be part of scheduler/netpoll, i.e.
call epoll_wait with the timers timeout."

https://github.com/golang/go/issues/25471#issuecomment-391906366
https://github.com/golang/go/issues/25471#issuecomment-555583843
https://github.com/golang/go/issues/27707#issuecomment-448859041

If delayed because no idle CPU, or no thread running, we are ok. What if
delayed because goroutine is supposed to run, but not getting to run because M
is not running and expected to be running. This does happen when process can't
use up to GOMAXPROCS.

Note that https://docs.google.com/document/d/1TTj4T2JO42uD5ID9e89oa0sLKhJYD0Y_kqxDv3I3XMw/edit#heading=h.mmq8lm48qfcw
says:
"When M is idle or in syscall, it does not need P."
"Similarly, when an M enters syscall, it must ensure that there is another M to execute Go code."

So doing a usleep or nanosleep will not prevent us from utilizing all the P's

On gceworker:
Histogram
[16.384µs,32.767µs): 3
[32.768µs,49.151µs): 7
[49.152µs,65.535µs): 4
[65.536µs,81.919µs): 190
[81.92µs,98.303µs): 27
[98.304µs,114.687µs): 204
[114.688µs,131.071µs): 81
[131.072µs,147.455µs): 52
[147.456µs,163.839µs): 383
[163.84µs,180.223µs): 881
[180.224µs,196.607µs): 398
[196.608µs,212.991µs): 134
[212.992µs,229.375µs): 699
[229.376µs,245.759µs): 110
[245.76µs,262.143µs): 87
[262.144µs,278.527µs): 17
[278.528µs,294.911µs): 113
[294.912µs,311.295µs): 104
[311.296µs,327.679µs): 276
[327.68µs,344.063µs): 291
[344.064µs,360.447µs): 159
[360.448µs,376.831µs): 83
[376.832µs,393.215µs): 42
[393.216µs,409.599µs): 114
[409.6µs,425.983µs): 385
[425.984µs,442.367µs): 3458
[442.368µs,458.751µs): 964
[458.752µs,475.135µs): 551
[475.136µs,491.519µs): 123
[491.52µs,507.903µs): 41
[507.904µs,524.287µs): 9
[524.288µs,540.671µs): 6
[540.672µs,557.055µs): 2
[786.432µs,802.815µs): 1
BenchmarkNumRunnableGoroutinesPlusNanoSleep-16    	   10000	    341279 ns/op	       0 B/op	       0 allocs/op

*/

/*
func BenchmarkNumRunnableGoroutinesPlusNanoSleep(b *testing.B) {
	var lastTime time.Time
	i := 0
	tickerDuration := 20 * time.Microsecond
	durations := hdrhistogram.New(int64(tickerDuration), int64(1000*tickerDuration), 3)
	spec := syscall.Timespec{
		Sec:  0,
		Nsec: int64(tickerDuration),
	}
	b.ResetTimer()
	for {
		now := timeutil.Now()
		if i > 0 {
			dur := now.Sub(lastTime)
			durations.RecordValue(int64(dur))
		}
		lastTime = now
		_, _ = numRunnableGoroutines()
		i++
		if i == b.N {
			break
		}
		syscall.Nanosleep(&spec, &spec)
	}
	b.StopTimer()
	bars := durations.Distribution()
	fmt.Printf("\nHistogram\n")
	for i := range bars {
		if bars[i].Count == 0 {
			continue
		}
		fmt.Printf("[%s,%s): %d\n", time.Duration(bars[i].From).String(),
			time.Duration(bars[i].To), bars[i].Count)
	}
}
*/

func BenchmarkNumRunnableGoroutinesPlusSched(b *testing.B) {
	var lastTime time.Time
	i := 0
	tickerDuration := 20 * time.Microsecond
	durations := hdrhistogram.New(int64(tickerDuration), int64(1000*tickerDuration), 3)
	b.ResetTimer()
	for {
		now := timeutil.Now()
		if i > 0 {
			dur := now.Sub(lastTime)
			durations.RecordValue(int64(dur))
		}
		lastTime = now
		_, _ = numRunnableGoroutines()
		i++
		if i == b.N {
			break
		}
		runtime.Gosched()
		// time.Sleep(tickerDuration)
	}
	b.StopTimer()
	bars := durations.Distribution()
	fmt.Printf("\nHistogram\n")
	for i := range bars {
		if bars[i].Count == 0 {
			continue
		}
		fmt.Printf("[%s,%s): %d\n", time.Duration(bars[i].From).String(),
			time.Duration(bars[i].To), bars[i].Count)
	}
}
