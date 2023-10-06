// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !linux
// +build !linux

package goschedstats

import "syscall"

func canNanoSleep() bool {
	return false
}

func sleepFor(spec syscall.Timespec) {
	panic("must not be called since cannot nanosleep")
}
