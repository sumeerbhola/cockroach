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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CumulativeNormalizedRunnableGoroutines returns the sum, over all seconds
// since the program started, of the average number of runnable goroutines per
// GOMAXPROC.
//
// Runnable goroutines are goroutines which are ready to run but are waiting for
// an available process. Sustained high numbers of waiting goroutines are a
// potential indicator of high CPU saturation (overload).
//
// The number of runnable goroutines is sampled frequently, and an average is
// calculated and accumulated once per second.
func CumulativeNormalizedRunnableGoroutines() float64 {
	rawValue := atomic.LoadUint64(&runnableInfo.total)

	procs := runtime.GOMAXPROCS(0)

	return float64(rawValue) * invScale / float64(procs)
}

// If you get a compilation error here, the Go version you are using is not
// supported by this package. Cross-check the structures in runtime_go1.15.go
// against those in the new Go's runtime, and if they are still accurate adjust
// the build tag in that file to accept the version. If they don't match, you
// will have to add a new version of that file.
var _ = numRunnableGoroutines

// We sample the number of runnable goroutines once per samplePeriod.
const samplePeriod = time.Millisecond

// We "report" the average value seen every reportingPeriod.
const reportingPeriod = time.Second

const scale = 1000
const invScale = 1.0 / scale

type RunnableCountCallback func(runnable int, maxProcs int)

var runnableInfo struct {
	// total accumulates the sum of the average number of runnable goroutines per
	// reportingPeriod, multiplied by scale.
	total uint64
	mu    syncutil.Mutex
	cb    RunnableCountCallback
}

func RegisterRunnableCountCallback(cb RunnableCountCallback) {
	runnableInfo.mu.Lock()
	defer runnableInfo.mu.Unlock()
	runnableInfo.cb = cb
}

func init() {
	go func() {
		lastTime := time.Now()
		sum := numRunnableGoroutines()
		numSamples := 1

		ticker := time.NewTicker(samplePeriod)
		for {
			t := <-ticker.C
			if t.Sub(lastTime) > reportingPeriod || t.Before(lastTime) {
				if numSamples > 0 {
					atomic.AddUint64(&runnableInfo.total, uint64(sum*scale/numSamples))
				}
				lastTime = t
				sum = 0
				numSamples = 0
			}
			nrg := numRunnableGoroutines()
			sum += nrg
			numSamples++
			procs := runtime.GOMAXPROCS(0)
			runnableInfo.mu.Lock()
			cb := runnableInfo.cb
			runnableInfo.mu.Unlock()
			if cb != nil {
				cb(nrg, procs)
			}
		}
	}()
}
