// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geomfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// s2GeometryIndex is an implementation of GeometryIndex that uses the S2 geometry
// library.
type s2GeometryIndex struct {
	rc                     *s2.RegionCoverer
	minX, maxX, minY, maxY float64
	deltaX, deltaY         float64
}

var _ GeometryIndex = (*s2GeometryIndex)(nil)

// We adjust the clipping bounds to be smaller by this fraction, since using
// the endpoints of face 0 in S2 causes coverings to spill out of that face.
const clippingBoundsDelta = 0.01

// NewS2GeometryIndex returns an index with the given configuration. All reads and
// writes on this index must use the same config. Writes must use the same
// config to correctly process deletions. Reads must use the same config since
// the bounds affect when a read needs to look at the exceedsBoundsCellID.
func NewS2GeometryIndex(cfg S2GeometryConfig) GeometryIndex {
	// TODO(sumeer): Sanity check cfg.
	return &s2GeometryIndex{
		rc: &s2.RegionCoverer{
			MinLevel: int(cfg.S2Config.MinLevel),
			MaxLevel: int(cfg.S2Config.MaxLevel),
			LevelMod: int(cfg.S2Config.LevelMod),
			MaxCells: int(cfg.S2Config.MaxCells),
		},
		minX:   cfg.MinX,
		maxX:   cfg.MaxX,
		minY:   cfg.MinY,
		maxY:   cfg.MaxY,
		deltaX: clippingBoundsDelta * (cfg.MaxX - cfg.MinX),
		deltaY: clippingBoundsDelta * (cfg.MaxY - cfg.MinY),
	}
}

// TODO(sumeer): also support index config with parameters specified by CREATE
// INDEX.

// DefaultGeometryIndexConfig returns a default config for a geometry index.
func DefaultGeometryIndexConfig() *Config {
	return &Config{
		S2Geometry: &S2GeometryConfig{
			// Arbitrary bounding box.
			MinX:     -10000,
			MaxX:     10000,
			MinY:     -10000,
			MaxY:     10000,
			S2Config: defaultS2Config()},
	}
}

// GeometryIndexConfigForSRID returns a geometry index config for srid.
func GeometryIndexConfigForSRID(srid geopb.SRID) (*Config, error) {
	if srid == 0 {
		return DefaultGeometryIndexConfig(), nil
	}
	p, exists := geoprojbase.Projection(srid)
	if !exists {
		return nil, errors.Newf("expected definition for SRID %d", srid)
	}
	b := p.Bounds
	minX, maxX, minY, maxY := b.MinX, b.MaxX, b.MinY, b.MaxY
	// There are projections where the min and max are equal e.g. 3571.
	// We need to have a valid rectangle as the geometry index bounds.
	if maxX-minX < 1 {
		maxX++
	}
	if maxY-minY < 1 {
		maxY++
	}
	// We are covering shapes using cells that are square. If we have shapes
	// that start of as well-behaved wrt square cells, we do not wish to
	// distort them significantly. Hence, we equalize MaxX-MinX and MaxY-MinY
	// in the index bounds.
	diffX := maxX - minX
	diffY := maxY - minY
	if diffX > diffY {
		adjustment := (diffX - diffY) / 2
		minY -= adjustment
		maxY += adjustment
	} else {
		adjustment := (diffY - diffX) / 2
		minX -= adjustment
		maxX += adjustment
	}

	// Expand the bounds by 2x the clippingBoundsDelta, to
	// ensure that shapes touching the bounds don't get
	// clipped.
	boundsExpansion := 2 * clippingBoundsDelta
	deltaX := (maxX - minX) * boundsExpansion
	deltaY := (maxY - minY) * boundsExpansion
	return &Config{
		S2Geometry: &S2GeometryConfig{
			MinX: minX - deltaX,
			MaxX: maxX + deltaX,
			MinY: minY - deltaY,
			MaxY: maxY + deltaY,
			/*
				MinX:     -9102387,
				MaxX:     11819889,
				MinY:     680961,
				MaxY:     9646533,
			*/
			S2Config: defaultS2Config()},
	}, nil
}

// A cell id unused by S2. We use it to index geometries that exceed the
// configured bounds.
const exceedsBoundsCellID = s2.CellID(^uint64(0))

var NumInvertedIndexKeys int64
var NumGeometryRows int64
var SumTightness float64
var SumBBoxTightness float64
var NumPoorTightness int64
var NumBadPolys int64
var NumFixedBadCoverings int64
var MinCellLevel int
var MaxCellLevel int
var CellLevelToCount [32]int

func keysToArea(keys []Key) float64 {
	var cu s2.CellUnion
	for _, k := range keys {
		cu = append(cu, s2.CellID(k))
	}
	cu.Normalize()
	return cu.ExactArea()
}

// TODO(sumeer): adjust code to handle precision issues with floating point
// arithmetic.

// covererWithBBoxFallback first computes the covering for the provided
// regions (which were computed using geom), and if the covering is too
// broad (contains faces other than 0), falls back to using the bounding
// box of geom to compute the covering.
type covererWithBBoxFallback struct {
	s    *s2GeometryIndex
	geom geom.T
}

var _ covererInterface = covererWithBBoxFallback{}

func (rc covererWithBBoxFallback) covering(regions []s2.Region) s2.CellUnion {
	cu := simpleCovererImpl{rc: rc.s.rc}.covering(regions)
	if isBadCovering(cu) {
		bbox := geo.BoundingBoxFromGeomTGeometryType(rc.geom)
		flatCoords := []float64{
			bbox.LoX, bbox.LoY, bbox.HiX, bbox.LoY, bbox.HiX, bbox.HiY, bbox.LoX, bbox.HiY,
			bbox.LoX, bbox.LoY}
		bboxT := geom.NewPolygonFlat(geom.XY, flatCoords, []int{len(flatCoords)})
		bboxRegions := rc.s.s2RegionsFromPlanarGeomT(bboxT)
		bboxCU := simpleCovererImpl{rc: rc.s.rc}.covering(bboxRegions)
		if !isBadCovering(bboxCU) {
			NumFixedBadCoverings++
			cu = bboxCU
		}
	}
	return cu
}

func isBadCovering(cu s2.CellUnion) bool {
	for _, c := range cu {
		if c.Face() != 0 {
			// Good coverings should not see a face other than 0.
			return true
		}
	}
	return false
}

// InvertedIndexKeys implements the GeometryIndex interface.
func (s *s2GeometryIndex) InvertedIndexKeys(c context.Context, g *geo.Geometry) ([]Key, error) {
	{
		// Log the WKB hex so we can match geometries seen by the query with what happened when
		// indexing the geometry by doing WKB hex string comparisons.
		wkb, err := geo.SpatialObjectToWKBHex(g.SpatialObject())
		if err != nil {
			panic(err)
		}
		log.Errorf(c, "wkbhex: %s", wkb)
		ewkt, err := geo.SpatialObjectToEWKT(g.SpatialObject(), 2)
		if err != nil {
			panic(err)
		}
		log.Errorf(c, "ewkt: %s", ewkt)
	}
	// If the geometry exceeds the bounds, we index the clipped geometry in
	// addition to the special cell, so that queries for geometries that don't
	// exceed the bounds don't need to query the special cell (which would
	// become a hotspot in the key space).
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var keys []Key
	var regionArea, coveringArea float64
	var tightness float64
	var bboxTightness float64
	var badPoly bool
	if gt != nil {
		gtArea := areaOfPlanarGeomT(gt)
		gtBboxArea := areaofPlanarGeomTBBox(gt)
		// log.Errorf(c, "gtArea: %f, gtBboxArea: %f", gtArea, gtBboxArea)
		if gtArea > 0 {
			bboxTightness = gtBboxArea / gtArea
		}
		r := s.s2RegionsFromPlanarGeomT(gt)
		for _, region := range r {
			polygon, ok := region.(*s2.Polygon)
			if ok {
				regionArea += polygon.Area()
			}
		}
		keys = invertedIndexKeys(c, covererWithBBoxFallback{s: s, geom: gt}, r)
		// keys = invertedIndexKeys(c, simpleCovererImpl{rc: s.rc}, r)
		for _, k := range keys {
			level := s2.CellID(k).Level()
			if level == 0 {
				NumBadPolys++
				badPoly = true
				break
			}
			CellLevelToCount[level]++
			if level > MaxCellLevel {
				MaxCellLevel = level
			}
			if level < MinCellLevel {
				MinCellLevel = level
			}
		}
		coveringArea = keysToArea(keys)
		if regionArea > 0 {
			tightness = coveringArea / regionArea
		}
		if tightness > 4 {
			NumPoorTightness++
		}
	}
	{
		// wkt, err := geo.SpatialObjectToWKT(g.SpatialObject(), 22)
		// if err != nil {
		//	panic(err)
		// }
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, "%s,", k)
		}
		log.Errorf(c, "cells: %s", b.String())
		log.Errorf(c, "index: bad: %t, tightness: %f, bbox tightness: %f, clipped: %t",
			badPoly, tightness, bboxTightness, clipped)
		SumTightness += tightness
		SumBBoxTightness += bboxTightness
		NumGeometryRows++
		NumInvertedIndexKeys += int64(len(keys))
		if NumGeometryRows%10000 == 0 {
			var b strings.Builder
			for level, count := range CellLevelToCount {
				fmt.Fprintf(&b, "L%d:%d,", level, count)
			}
			log.Errorf(c, "NumGeometryRows: %d, NumInvertedIndexKeys: %d, Mean Cells: %f, "+
				"Mean Area Ratio: %f, Mean BBox Area Ratio: %f, NumPoorTightness: %d, NumBadPolys: %d, "+
				"NumFixedBadCoverings: %d, LevelToCount: %s",
				NumGeometryRows, NumInvertedIndexKeys, float64(NumInvertedIndexKeys)/float64(NumGeometryRows),
				SumTightness/float64(NumGeometryRows), SumBBoxTightness/float64(NumGeometryRows),
				NumPoorTightness, NumBadPolys, NumFixedBadCoverings,
				b.String())
			log.Errorf(c, "index: minX: %f, minY: %f, maxX: %f, maxY: %f, deltaX: %f, deltaY: %f",
				s.minX, s.minY, s.maxX, s.maxY, s.deltaX, s.deltaY)
		}
	}
	if clipped {
		keys = append(keys, Key(exceedsBoundsCellID))
	}
	return keys, nil
}

// Covers implements the GeometryIndex interface.
func (s *s2GeometryIndex) Covers(c context.Context, g *geo.Geometry) (UnionKeySpans, error) {
	return s.Intersects(c, g)
}

// CoveredBy implements the GeometryIndex interface.
func (s *s2GeometryIndex) CoveredBy(c context.Context, g *geo.Geometry) (RPKeyExpr, error) {
	// If the geometry exceeds the bounds, we use the clipped geometry to
	// restrict the search within the bounds.
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var expr RPKeyExpr
	if gt != nil {
		r := s.s2RegionsFromPlanarGeomT(gt)
		expr = coveredBy(c, s.rc, r)
	}
	if clipped {
		// Intersect with the shapes that exceed the bounds.
		expr = append(expr, Key(exceedsBoundsCellID))
		if len(expr) > 1 {
			expr = append(expr, RPSetIntersection)
		}
	}
	return expr, nil
}

var IntersectsRows int64
var IntersectsSumTightness float64
var IntersectsSumBBoxTightness float64
var IntersectsNumBadPolys int64
var IntersectsClippedPolys int64

// Intersects implements the GeometryIndex interface.
func (s *s2GeometryIndex) Intersects(c context.Context, g *geo.Geometry) (UnionKeySpans, error) {
	// If the geometry exceeds the bounds, we use the clipped geometry to
	// restrict the search within the bounds.
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var spans UnionKeySpans
	points := 0
	var bboxTightness float64
	if gt != nil {
		gtArea := areaOfPlanarGeomT(gt)
		gtBboxArea := areaofPlanarGeomTBBox(gt)
		if gtArea > 0 {
			bboxTightness = gtBboxArea / gtArea
		}
		IntersectsSumBBoxTightness += bboxTightness
		points = len(gt.FlatCoords()) / 2
		r := s.s2RegionsFromPlanarGeomT(gt)
		spans = intersects(c, covererWithBBoxFallback{s: s, geom: gt}, r)
	}
	if clipped {
		IntersectsClippedPolys++
		log.Errorf(c, "Intersects: clipped: %t, num points: %d, bbox tightness: %f",
			clipped, points, bboxTightness)
		// And lookup all shapes that exceed the bounds. The exceedsBoundsCellID is the largest
		// possible key, so appending it maintains the sorted order of spans.
		spans = append(spans, KeySpan{Start: Key(exceedsBoundsCellID), End: Key(exceedsBoundsCellID)})
	}
	return spans, nil
}

func (s *s2GeometryIndex) DWithin(
	c context.Context, g *geo.Geometry, distance float64,
) (UnionKeySpans, error) {
	// TODO(sumeer): are the default params the correct thing to use here?
	g, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), distance)
	if err != nil {
		return nil, err
	}
	return s.Intersects(c, g)
}

func (s *s2GeometryIndex) DFullyWithin(
	c context.Context, g *geo.Geometry, distance float64,
) (UnionKeySpans, error) {
	// TODO(sumeer): are the default params the correct thing to use here?
	g, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), distance)
	if err != nil {
		return nil, err
	}
	return s.Covers(c, g)
}

// Converts to geom.T and clips to the rectangle bounds of the index.
func (s *s2GeometryIndex) convertToGeomTAndTryClip(g *geo.Geometry) (geom.T, bool, error) {
	gt, err := g.AsGeomT()
	if err != nil {
		return nil, false, err
	}
	if gt.Empty() {
		return gt, false, nil
	}
	clipped := false
	if s.geomExceedsBounds(gt) {
		clipped = true
		clippedEWKB, err :=
			geos.ClipEWKBByRect(g.EWKB(), s.minX+s.deltaX, s.minY+s.deltaY, s.maxX-s.deltaX, s.maxY-s.deltaY)
		if err != nil {
			return nil, false, err
		}
		gt = nil
		if clippedEWKB != nil {
			g, err = geo.ParseGeometryFromEWKBUnsafe(clippedEWKB)
			if err != nil {
				return nil, false, err
			}
			if g == nil {
				return nil, false, errors.Errorf("internal error: clippedWKB cannot be parsed")
			}
			gt, err = g.AsGeomT()
			if err != nil {
				return nil, false, err
			}
		}
	}
	return gt, clipped, nil
}

// Returns true if the point represented by (x, y) exceeds the rectangle
// bounds of the index.
func (s *s2GeometryIndex) xyExceedsBounds(x float64, y float64) bool {
	if x < (s.minX+s.deltaX) || x > (s.maxX-s.deltaX) {
		return true
	}
	if y < (s.minY+s.deltaY) || y > (s.maxY-s.deltaY) {
		return true
	}
	return false
}

// Returns true if g exceeds the rectangle bounds of the index.
func (s *s2GeometryIndex) geomExceedsBounds(g geom.T) bool {
	switch repr := g.(type) {
	case *geom.Point:
		return s.xyExceedsBounds(repr.X(), repr.Y())
	case *geom.LineString:
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			if s.xyExceedsBounds(p.X(), p.Y()) {
				return true
			}
		}
	case *geom.Polygon:
		if repr.NumLinearRings() > 0 {
			lr := repr.LinearRing(0)
			for i := 0; i < lr.NumCoords(); i++ {
				if s.xyExceedsBounds(lr.Coord(i).X(), lr.Coord(i).Y()) {
					return true
				}
			}
		}
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			if s.geomExceedsBounds(geom) {
				return true
			}
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			if s.geomExceedsBounds(repr.Point(i)) {
				return true
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			if s.geomExceedsBounds(repr.LineString(i)) {
				return true
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			if s.geomExceedsBounds(repr.Polygon(i)) {
				return true
			}
		}
	}
	return false
}

// stToUV() and face0UVToXYZPoint() are adapted from unexported methods in
// github.com/golang/geo/s2/stuv.go

// stToUV converts an s or t value to the corresponding u or v value.
// This is a non-linear transformation from [-1,1] to [-1,1] that
// attempts to make the cell sizes more uniform.
// This uses what the C++ version calls 'the quadratic transform'.
func stToUV(s float64) float64 {
	if s >= 0.5 {
		return (1 / 3.) * (4*s*s - 1)
	}
	return (1 / 3.) * (1 - 4*(1-s)*(1-s))
}

// Specialized version of faceUVToXYZ() for face 0
func face0UVToXYZPoint(u, v float64) s2.Point {
	return s2.Point{Vector: r3.Vector{X: 1, Y: u, Z: v}}
}

func (s *s2GeometryIndex) planarPointToS2Point(x float64, y float64) s2.Point {
	ss := (x - s.minX) / (s.maxX - s.minX)
	tt := (y - s.minY) / (s.maxY - s.minY)
	u := stToUV(ss)
	v := stToUV(tt)
	return face0UVToXYZPoint(u, v)
}

func areaofPlanarGeomTBBox(geomRepr geom.T) float64 {
	bbox := geo.BoundingBoxFromGeomTGeometryType(geomRepr)
	if bbox == nil {
		return 0
	}
	area := (bbox.HiY - bbox.LoY) * (bbox.HiX - bbox.LoX)
	if area < 0 {
		log.Errorf(context.Background(), "negative area: %f", area)
		area = 0
	}
	return area
}

func areaOfPlanarGeomT(geomRepr geom.T) float64 {
	if geomRepr.Empty() {
		return 0
	}
	var area float64
	switch repr := geomRepr.(type) {
	case *geom.Point:
		return 0
	case *geom.LineString:
		return 0
	case *geom.Polygon:
		return math.Abs(repr.Area())
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			area += areaOfPlanarGeomT(geom)
		}
	case *geom.MultiPoint:
		return 0
	case *geom.MultiLineString:
		return 0
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			area += math.Abs(repr.Polygon(i).Area())
		}
	}
	return area
}

// TODO(sumeer): this is similar to S2RegionsFromGeomT() but needs to do
// a different point conversion. If these functions do not diverge further,
// and turn out not to be performance critical, merge the two implementations.
func (s *s2GeometryIndex) s2RegionsFromPlanarGeomT(geomRepr geom.T) []s2.Region {
	if geomRepr.Empty() {
		return nil
	}
	var regions []s2.Region
	switch repr := geomRepr.(type) {
	case *geom.Point:
		regions = []s2.Region{
			s.planarPointToS2Point(repr.X(), repr.Y()),
		}
	case *geom.LineString:
		points := make([]s2.Point, repr.NumCoords())
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			points[i] = s.planarPointToS2Point(p.X(), p.Y())
		}
		pl := s2.Polyline(points)
		regions = []s2.Region{&pl}
	case *geom.Polygon:
		loops := make([]*s2.Loop, repr.NumLinearRings())
		// The first ring is a "shell". Following rings are "holes".
		// All loops must be oriented CCW for S2.
		for ringIdx := 0; ringIdx < repr.NumLinearRings(); ringIdx++ {
			linearRing := repr.LinearRing(ringIdx)
			points := make([]s2.Point, linearRing.NumCoords())
			isCCW := geo.IsLinearRingCCW(linearRing)
			for pointIdx := 0; pointIdx < linearRing.NumCoords(); pointIdx++ {
				p := linearRing.Coord(pointIdx)
				pt := s.planarPointToS2Point(p.X(), p.Y())
				if isCCW {
					points[pointIdx] = pt
				} else {
					points[len(points)-pointIdx-1] = pt
				}
			}
			loops[ringIdx] = s2.LoopFromPoints(points)
		}
		regions = []s2.Region{
			s2.PolygonFromLoops(loops),
		}
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			regions = append(regions, s.s2RegionsFromPlanarGeomT(geom)...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeomT(repr.Point(i))...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeomT(repr.LineString(i))...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeomT(repr.Polygon(i))...)
		}
	}
	return regions
}

func (s *s2GeometryIndex) TestingInnerCovering(g *geo.Geometry) s2.CellUnion {
	gt, _, err := s.convertToGeomTAndTryClip(g)
	if err != nil || gt == nil {
		return nil
	}
	r := s.s2RegionsFromPlanarGeomT(gt)
	return innerCovering(s.rc, r)
}
