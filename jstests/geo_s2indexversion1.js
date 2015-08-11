// Tests 2dsphere index option "2dsphereIndexVersion".  Verifies that only index version 1 is
// permitted.

var coll = db.getCollection("geo_s2indexversion1");
coll.drop();

//
// Index build should fail for invalid values of "2dsphereIndexVersion".
//

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": -1});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": 0});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": 2});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": 3});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": Infinity});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": "foo"});
assert.gleError(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": {a: 1}});
assert.gleError(db);
coll.drop();

//
// Index build should succeed for valid values of "2dsphereIndexVersion".
//

coll.ensureIndex({geo: "2dsphere"});
assert.gleSuccess(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": 1});
assert.gleSuccess(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": NumberInt(1)});
assert.gleSuccess(db);
coll.drop();

coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": NumberLong(1)});
assert.gleSuccess(db);
coll.drop();

//
// Test compatibility of various GeoJSON objects with 2dsphere.
//

var pointDoc = {geo: {type: "Point", coordinates: [40, 5]}};
var lineStringDoc = {geo: {type: "LineString", coordinates: [[40, 5], [41, 6]]}};
var polygonDoc = {geo: {type: "Polygon", coordinates: [[[0, 0], [3, 6], [6, 1], [0, 0]]]}};
var multiPointDoc = {geo: {type: "MultiPoint",
                           coordinates: [[-73.9580, 40.8003], [-73.9498, 40.7968],
                                         [-73.9737, 40.7648], [-73.9814, 40.7681]]}};
var multiLineStringDoc = {geo: {type: "MultiLineString",
                                coordinates: [[[-73.96943, 40.78519], [-73.96082, 40.78095]],
                                              [[-73.96415, 40.79229], [-73.95544, 40.78854]],
                                              [[-73.97162, 40.78205], [-73.96374, 40.77715]],
                                              [[-73.97880, 40.77247], [-73.97036, 40.76811]]]}};
var multiPolygonDoc = {geo: {type: "MultiPolygon",
                             coordinates: [[[[-73.958, 40.8003], [-73.9498, 40.7968],
                                             [-73.9737, 40.7648], [-73.9814, 40.7681],
                                             [-73.958, 40.8003]]],
                                           [[[-73.958, 40.8003], [-73.9498, 40.7968],
                                             [-73.9737, 40.7648], [-73.958, 40.8003]]]]}};
var geometryCollectionDoc = {geo: {type: "GeometryCollection",
                                   geometries: [{type: "MultiPoint",
                                                 coordinates: [[-73.9580, 40.8003],
                                                               [-73.9498, 40.7968],
                                                               [-73.9737, 40.7648],
                                                               [-73.9814, 40.7681]]},
                                                {type: "MultiLineString",
                                                 coordinates: [[[-73.96943, 40.78519],
                                                                [-73.96082, 40.78095]],
                                                               [[-73.96415, 40.79229],
                                                                [-73.95544, 40.78854]],
                                                               [[-73.97162, 40.78205],
                                                                [-73.96374, 40.77715]],
                                                               [[-73.97880, 40.77247],
                                                                [-73.97036, 40.76811]]]}]}};

// {2dsphereIndexVersion: 1} indexes allow only Point, LineString, and Polygon.
coll.ensureIndex({geo: "2dsphere"}, {"2dsphereIndexVersion": 1});
assert.gleSuccess(db);
coll.insert(pointDoc);
assert.gleSuccess(db);
coll.insert(lineStringDoc);
assert.gleSuccess(db);
coll.insert(polygonDoc);
assert.gleSuccess(db);
coll.insert(multiPointDoc);
assert.gleError(db);
coll.insert(multiLineStringDoc);
assert.gleError(db);
coll.insert(multiPolygonDoc);
assert.gleError(db);
coll.insert(geometryCollectionDoc);
assert.gleError(db);
coll.drop();
