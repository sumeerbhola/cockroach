// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geopb

// The following are the common standard SRIDs that we support.
const (
	// UnknownSRID is the default SRID if none is provided.
	UnknownSRID = SRID(0)
	// DefaultGeometrySRID is the same as being unknown.
	DefaultGeometrySRID = UnknownSRID
	// DefaultGeographySRID (aka 4326) is the GPS lat/lng we all know and love.
	// In this system, (long, lat) corresponds to (X, Y), bounded by
	// ([-180, 180], [-90 90]).
	DefaultGeographySRID = SRID(4326)
)

// SRID is a Spatial Reference Identifer. All geometry and geography shapes are
// stored and represented as using coordinates that are bare floats. SRIDs tie these
// floats to the planar or spherical coordinate system, allowing them to be interpreted
// and compared.
//
// The zero value is special and means an unknown coordinate system.
type SRID int32

// WKT is the Well Known Text form of a spatial object.
type WKT string

// EWKT is the Extended Well Known Text form of a spatial object.
type EWKT string

// WKB is the Well Known Bytes form of a spatial object.
type WKB []byte

// EWKB is the Extended Well Known Bytes form of a spatial object.
type EWKB []byte

// ShapeType is the type of a spatial shape. Each of these corresponds to a
// different representation and serialization format. For example, a Point is a
// pair of doubles (or more than that for geometries with Z or N), a LineString
// is an ordered series of Points, etc.
type ShapeType int16

const (
	ShapeType_Unset           ShapeType = 0
	ShapeType_Point           ShapeType = 1
	ShapeType_LineString      ShapeType = 2
	ShapeType_Polygon         ShapeType = 3
	ShapeType_MultiPoint      ShapeType = 4
	ShapeType_MultiLineString ShapeType = 5
	ShapeType_MultiPolygon    ShapeType = 6
	// Geometry can contain any type.
	ShapeType_Geometry ShapeType = 7
	// GeometryCollection can contain a list of any above type except for Geometry.
	ShapeType_GeometryCollection ShapeType = 8
)

var ShapeType_name = map[int16]string{
	0: "Unset",
	1: "Point",
	2: "LineString",
	3: "Polygon",
	4: "MultiPoint",
	5: "MultiLineString",
	6: "MultiPolygon",
	7: "Geometry",
	8: "GeometryCollection",
}
var ShapeType_value = map[string]int16{
	"Unset":              0,
	"Point":              1,
	"LineString":         2,
	"Polygon":            3,
	"MultiPoint":         4,
	"MultiLineString":    5,
	"MultiPolygon":       6,
	"Geometry":           7,
	"GeometryCollection": 8,
}

func (x ShapeType) String() string {
	return ShapeType_name[int16(x)]
}

// SpatialObjectType represents the type of the SpatialObject.
type SpatialObjectType int16

const (
	SpatialObjectType_Unknown       SpatialObjectType = 0
	SpatialObjectType_GeographyType SpatialObjectType = 1
	SpatialObjectType_GeometryType  SpatialObjectType = 2
)

var SpatialObjectType_name = map[int16]string{
	0: "Unknown",
	1: "GeographyType",
	2: "GeometryType",
}
var SpatialObjectType_value = map[string]int16{
	"Unknown":       0,
	"GeographyType": 1,
	"GeometryType":  2,
}

func (x SpatialObjectType) String() string {
	return SpatialObjectType_name[int16(x)]
}
