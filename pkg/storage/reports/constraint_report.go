// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// replicationConstraintsReportID is the id of the row in the system.
// reports_meta table corresponding to the constraints conformance report (i.e.
// the system.replicationConstraintsReportID table).
const replicationConstraintsReportID = 1

// ConstraintReport contains information about the constraint conformance for
// the cluster's data.
type ConstraintReport map[ConstraintStatusKey]ConstraintStatus

// ReplicationConstraintStatsReportSaver manages the content and the saving of the report.
type replicationConstraintStatsReportSaver struct {
	previousVersion     ConstraintReport
	lastGenerated       time.Time
	lastUpdatedRowCount int

	constraints ConstraintReport
}

// makeReplicationConstraintStatusReportSaver creates a new report saver.
func makeReplicationConstraintStatusReportSaver() replicationConstraintStatsReportSaver {
	return replicationConstraintStatsReportSaver{
		constraints: ConstraintReport{},
	}
}

// resetReport resets the report to an empty state.
func (r *replicationConstraintStatsReportSaver) resetReport() {
	r.constraints = ConstraintReport{}
}

// LastUpdatedRowCount is the count of the rows that were touched during the last save.
func (r *replicationConstraintStatsReportSaver) LastUpdatedRowCount() int {
	return r.lastUpdatedRowCount
}

// ConstraintStatus is the leaf in the constraintReport.
type ConstraintStatus struct {
	FailRangeCount int
}

// ConstraintType indicates what type of constraint is an entry in the
// constraint conformance report talking about.
type ConstraintType string

const (
	// Constraint means that the entry refers to a constraint (i.e. a member of
	// the constraints field in a zone config).
	Constraint ConstraintType = "constraint"
	// TODO(andrei): add leaseholder preference
)

// Less compares two ConstraintTypes.
func (t ConstraintType) Less(other ConstraintType) bool {
	return -1 == strings.Compare(string(t), string(other))
}

// ConstraintRepr is a string representation of a constraint.
type ConstraintRepr string

// Less compares two ConstraintReprs.
func (c ConstraintRepr) Less(other ConstraintRepr) bool {
	return -1 == strings.Compare(string(c), string(other))
}

// ConstraintStatusKey represents the key in the ConstraintReport.
type ConstraintStatusKey struct {
	ZoneKey
	ViolationType ConstraintType
	Constraint    ConstraintRepr
}

func (k ConstraintStatusKey) String() string {
	return fmt.Sprintf("zone:%s type:%s constraint:%s", k.ZoneKey, k.ViolationType, k.Constraint)
}

// Less compares two ConstraintStatusKeys.
func (k ConstraintStatusKey) Less(other ConstraintStatusKey) bool {
	if k.ZoneKey.Less(other.ZoneKey) {
		return true
	}
	if other.ZoneKey.Less(k.ZoneKey) {
		return false
	}
	if k.ViolationType.Less(other.ViolationType) {
		return true
	}
	if other.ViolationType.Less(k.ViolationType) {
		return true
	}
	return k.Constraint.Less(other.Constraint)
}

// MakeConstraintRepr creates a canonical string representation for a
// constraint. The constraint is identified by the group it belongs to and the
// index within the group.
func MakeConstraintRepr(constraintGroup config.Constraints, constraintIdx int) ConstraintRepr {
	cstr := constraintGroup.Constraints[constraintIdx].String()
	if constraintGroup.NumReplicas == 0 {
		return ConstraintRepr(cstr)
	}
	return ConstraintRepr(fmt.Sprintf("%q:%d", cstr, constraintGroup.NumReplicas))
}

// AddViolation add a constraint that is being violated for a given range. Each call
// will increase the number of ranges that failed.
func (r *replicationConstraintStatsReportSaver) AddViolation(
	z ZoneKey, t ConstraintType, c ConstraintRepr,
) {
	k := ConstraintStatusKey{
		ZoneKey:       z,
		ViolationType: t,
		Constraint:    c,
	}
	if _, ok := r.constraints[k]; !ok {
		r.constraints[k] = ConstraintStatus{}
	}
	cRep := r.constraints[k]
	cRep.FailRangeCount++
	r.constraints[k] = cRep
}

// EnsureEntry us used to add an entry to the report even if there is no violation.
func (r *replicationConstraintStatsReportSaver) EnsureEntry(
	z ZoneKey, t ConstraintType, c ConstraintRepr,
) {
	k := ConstraintStatusKey{
		ZoneKey:       z,
		ViolationType: t,
		Constraint:    c,
	}
	if _, ok := r.constraints[k]; !ok {
		r.constraints[k] = ConstraintStatus{}
	}
}

func (r *replicationConstraintStatsReportSaver) ensureEntries(
	key ZoneKey, zone *config.ZoneConfig,
) {
	for _, group := range zone.Constraints {
		for i := range group.Constraints {
			r.EnsureEntry(key, Constraint, MakeConstraintRepr(group, i))
		}
	}
	for i, sz := range zone.Subzones {
		szKey := ZoneKey{ZoneID: key.ZoneID, SubzoneID: SubzoneIDFromIndex(i)}
		r.ensureEntries(szKey, &sz.Config)
	}
}

func (r *replicationConstraintStatsReportSaver) loadPreviousVersion(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *client.Txn,
) error {
	// The data for the previous save needs to be loaded if:
	// - this is the first time that we call this method and lastUpdatedAt has never been set
	// - in case that the lastUpdatedAt is set but is different than the timestamp in reports_meta
	//   this indicates that some other worker wrote after we did the write.
	if !r.lastGenerated.IsZero() {
		// check to see if the last timestamp for the update matches the local one.
		row, err := ex.QueryRow(
			ctx,
			"get-previous-timestamp",
			txn,
			"select generated from system.reports_meta where id = $1",
			replicationConstraintsReportID,
		)
		if err != nil {
			return err
		}

		// if the row is nil then this is the first time we are running and the reload is needed.
		if row != nil {
			generated, ok := row[0].(*tree.DTimestamp)
			if !ok {
				return errors.Errorf("Expected to get time from system.reports_meta but got %+v", row)
			}
			if generated.Time == r.lastGenerated {
				// No need to reload.
				return nil
			}
		}
	}
	const prevViolations = "select zone_id, subzone_id, type, config, " +
		"violating_ranges from system.replication_constraint_stats"
	rows, err := ex.Query(
		ctx, "get-previous-replication-constraint-stats", txn, prevViolations,
	)
	if err != nil {
		return err
	}

	r.previousVersion = make(ConstraintReport, len(rows))
	for _, row := range rows {
		key := ConstraintStatusKey{}
		key.ZoneID = (uint32)(*row[0].(*tree.DInt))
		key.SubzoneID = SubzoneID((*row[1].(*tree.DInt)))
		key.ViolationType = (ConstraintType)(*row[2].(*tree.DString))
		key.Constraint = (ConstraintRepr)(*row[3].(*tree.DString))
		r.previousVersion[key] = ConstraintStatus{(int)(*row[4].(*tree.DInt))}
	}

	return nil
}

func (r *replicationConstraintStatsReportSaver) updatePreviousVersion() {
	r.previousVersion = r.constraints
	r.constraints = make(ConstraintReport, len(r.previousVersion))
}

func (r *replicationConstraintStatsReportSaver) updateTimestamp(
	ctx context.Context, ex sqlutil.InternalExecutor, txn *client.Txn, reportTS time.Time,
) error {
	if !r.lastGenerated.IsZero() && reportTS == r.lastGenerated {
		return errors.Errorf(
			"The new time %s is the same as the time of the last update %s",
			reportTS.String(),
			r.lastGenerated.String(),
		)
	}

	_, err := ex.Exec(
		ctx,
		"timestamp-upsert-replication-constraint-stats",
		txn,
		"upsert into system.reports_meta(id, generated) values($1, $2)",
		replicationConstraintsReportID,
		reportTS,
	)
	return err
}

// Save the report.
//
// reportTS is the time that will be set in the updated_at column for every row.
func (r *replicationConstraintStatsReportSaver) Save(
	ctx context.Context, reportTS time.Time, db *client.DB, ex sqlutil.InternalExecutor,
) error {
	r.lastUpdatedRowCount = 0
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		err := r.loadPreviousVersion(ctx, ex, txn)
		if err != nil {
			return err
		}

		err = r.updateTimestamp(ctx, ex, txn, reportTS)
		if err != nil {
			return err
		}

		for k, zoneCons := range r.constraints {
			if err := r.upsertConstraintStatus(
				ctx, reportTS, txn, k, zoneCons.FailRangeCount, db, ex,
			); err != nil {
				return err
			}
		}

		for key := range r.previousVersion {
			if _, ok := r.constraints[key]; !ok {
				_, err := ex.Exec(
					ctx,
					"delete-old-replication-constraint-stats",
					txn,
					"delete from system.replication_constraint_stats "+
						"where zone_id = $1 and subzone_id = $2 and type = $3 and config = $4",
					key.ZoneID,
					key.SubzoneID,
					key.ViolationType,
					key.Constraint,
				)

				if err != nil {
					return err
				}
				r.lastUpdatedRowCount++
			}
		}

		return nil
	}); err != nil {
		return err
	}

	r.lastGenerated = reportTS
	r.updatePreviousVersion()

	return nil
}

// upsertConstraintStatus upserts a row into system.replication_constraint_stats.
//
// existing is used to decide is this is a new violation.
func (r *replicationConstraintStatsReportSaver) upsertConstraintStatus(
	ctx context.Context,
	reportTS time.Time,
	txn *client.Txn,
	key ConstraintStatusKey,
	violationCount int,
	db *client.DB,
	ex sqlutil.InternalExecutor,
) error {
	var err error
	previousStatus, hasOldVersion := r.previousVersion[key]
	if hasOldVersion && previousStatus.FailRangeCount == violationCount {
		// No change in the status so no update.
		return nil
	} else if violationCount != 0 {
		if previousStatus.FailRangeCount != 0 {
			// Updating an old violation. No need to update the start timestamp.
			_, err = ex.Exec(
				ctx, "upsert-replication-constraint-stat", txn,
				"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, "+
					"config, violating_ranges) values($1, $2, $3, $4, $5, $6)",
				replicationConstraintsReportID,
				key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount,
			)
		} else if previousStatus.FailRangeCount == 0 {
			// New violation detected. Need to update the start timestamp.
			_, err = ex.Exec(
				ctx, "upsert-replication-constraint-stat", txn,
				"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, "+
					"config, violating_ranges, violation_start) values($1, $2, $3, $4, $5, $6, $7)",
				replicationConstraintsReportID,
				key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount, reportTS,
			)
		}
	} else {
		// Need to set the violation start to null as there was an violation that doesn't exist anymore.
		_, err = ex.Exec(
			ctx, "upsert-replication-constraint-stat", txn,
			"upsert into system.replication_constraint_stats(report_id, zone_id, subzone_id, type, config, "+
				"violating_ranges, violation_start) values($1, $2, $3, $4, $5, $6, null)",
			replicationConstraintsReportID,
			key.ZoneID, key.SubzoneID, key.ViolationType, key.Constraint, violationCount,
		)
	}

	if err != nil {
		return err
	}

	r.lastUpdatedRowCount++
	return nil
}

// constraintConformanceVisitor is a visitor that, when passed to visitRanges(),
// computes the constraint conformance report (i.e. the
// system.replication_constraint_stats table).
type constraintConformanceVisitor struct {
	cfg           *config.SystemConfig
	storeResolver StoreResolver

	report *replicationConstraintStatsReportSaver
}

var _ rangeVisitor = &constraintConformanceVisitor{}

func makeConstraintConformanceVisitor(
	ctx context.Context,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
	saver *replicationConstraintStatsReportSaver,
) constraintConformanceVisitor {
	v := constraintConformanceVisitor{
		cfg:           cfg,
		storeResolver: storeResolver,
		report:        saver,
	}
	v.reset(ctx)
	return v
}

// reset is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) reset(ctx context.Context) {
	v.report.resetReport()

	// Iterate through all the zone configs to create report entries for all the
	// zones that have constraints. Otherwise, just iterating through the ranges
	// wouldn't create entries for constraints that aren't violated, and
	// definitely not for zones that don't apply to any ranges.
	maxObjectID, err := v.cfg.GetLargestObjectID(0 /* maxID - return the largest ID in the config */)
	if err != nil {
		log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
	}
	for i := uint32(1); i <= maxObjectID; i++ {
		zone, err := getZoneByID(i, v.cfg)
		if err != nil {
			log.Fatalf(ctx, "unexpected failure to compute max object id: %s", err)
		}
		if zone == nil {
			continue
		}
		v.report.ensureEntries(MakeZoneKey(i, NoSubzone), zone)
	}
}

// constraintConformanceVisitor is part of the rangeVisitor interface.
func (v *constraintConformanceVisitor) visit(ctx context.Context, r roachpb.RangeDescriptor) {
	storeDescs := v.storeResolver(r)

	// Find the applicable constraints, which may be inherited.
	var constraints []config.Constraints
	var zKey ZoneKey
	err := visitZones(ctx, r, v.cfg,
		func(_ context.Context, zone *config.ZoneConfig, key ZoneKey) bool {
			if zone.Constraints == nil {
				return false
			}
			constraints = zone.Constraints
			zKey = key
			return true
		})
	if err != nil {
		log.Fatalf(ctx, "unexpected error visiting zones: %s", err)
	}

	violated := processRange(ctx, storeDescs, constraints)
	for _, c := range violated {
		v.report.AddViolation(zKey, Constraint, c)
	}
}
