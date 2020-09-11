// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logictest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestLogic runs logic tests that were written by hand to test various
// CockroachDB features. The tests use a similar methodology to the SQLLite
// Sqllogictests. All of these tests should only verify correctness of output,
// and not how that output was derived. Therefore, these tests can be run
// using the heuristic planner, the cost-based optimizer, or even run against
// Postgres to verify it returns the same logical results.
//
// See the comments in logic.go for more details.
func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	RunLogicTest(t, TestServerArgs{}, "testdata/logic_test/inverted_filter*")
}

// TestSqlLiteLogic runs the supported SqlLite logic tests. See the comments
// for runSQLLiteLogicTest for more detail on these tests.
func TestSqlLiteLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	RunSQLLiteLogicTest(t, "" /* configOverride */)
}
