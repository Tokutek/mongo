// test that cursors that require a sort work properly
Random.setRandomSeed();
tn = "partition_sorted_cursor";
tn2 = "normal_sorted_cursor";
t = db[tn];
t2 = db[tn2];
t.drop();
t2.drop();


createColls = function () {
    assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {ts:1, _id:1}}));
    assert.commandWorked(db.createCollection(tn2, {primaryKey : {ts:1, _id:1}}));
}

dropColls = function() {
    t.drop();
    t2.drop();
}

verifyCursorsEqual = function(x, y) {
    while (x.hasNext()) {
        assert(y.hasNext());
        first = x.next();
        second = y.next();
        //printjson(first);
        //printjson(second);
        assert(friendlyEqual(first, second));
    }
    assert(!y.hasNext());
}

runQuery = function(query, index) {
    //printjson(index);
    explainOutput = t.find(query).sort({a:1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // now let's compare the values
    x = t.find(query).sort({a:1}).hint(index);
    y = t2.find(query).sort({a:1}).hint(index);
    verifyCursorsEqual(x,y);

    explainOutput = t.find(query).sort({a:-1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // now let's compare the values
    x = t.find(query).sort({a:-1}).hint(index);
    y = t2.find(query).sort({a:-1}).hint(index);
    verifyCursorsEqual(x,y);
    assert(t.count(query) == t2.count(query));
}

runQueryWithProjection = function(query, projection, index) {
    //printjson(index);
    explainOutput = t.find(query, projection).sort({a:1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // now let's compare the values
    x = t.find(query, projection).sort({a:1}).hint(index);
    y = t2.find(query, projection).sort({a:1}).hint(index);
    verifyCursorsEqual(x,y);

    explainOutput = t.find(query, projection).sort({a:-1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // now let's compare the values
    x = t.find(query, projection).sort({a:-1}).hint(index);
    y = t2.find(query, projection).sort({a:-1}).hint(index);
    verifyCursorsEqual(x,y);
    assert(t.count(query) == t2.count(query));
}

runSomeQueries = function(index) {
    // index scan query
    runQuery({}, index);
    runQuery({a : {$gte: 25}}, index);
    runQuery({a : {$lte: 75}}, index);
    runQuery( {$and : [ {a : {$gte: 25}} , {a : {$lte: 75}} ] }, index );
    runQuery({a : {$gte: 1000}}, index); // query with no results
    runQuery( {$and : [ {a : {$gte: 25}} , {a : {$lte: 75}}, {b : {$gt : 100}} ] }, index ); // query with a matcher
    runQueryWithProjection( {$and : [ {a : {$gte: 25}} , {a : {$lte: 75}}, {b : {$gt : 100}} ] }, {a:1, b:1}, index );

    // some tests where there ought to be filters on the pk
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 75}} ] }, index );
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$gte: 25}} ] }, index );
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 75}}, {ts : {$gte : 25}} ] }, index );
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 70}}, {ts : {$gte : 70}} ] }, index ); // case where it lies on border of partition
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 70}}, {ts : {$gte : 20}} ] }, index ); // case where it lies on border of partition
    runQuery( {$and : [ {a : {$gte: 25}} , {$or : [{ts : {$gte: 85}}, {ts : {$lte : 15}}]} ] }, index ); // non-overlapping or
    runQuery( {$and : [ {a : {$gte: 25}} , {$or : [{ts : {$gte: 35}}, {ts : {$lte : 55}}]} ] }, index ); // overlapping or
    runQuery( {$and : [ {a : {$gte: 25}} ,
                        {$or : [  {$and : [ {ts : {$gte: 35}}, {ts : {$lte: 55}}]} ,
                                  {$and : [ {ts : {$gte: 45}}, {ts : {$lte: 75}}]}
                               ]
                         }
                      ]
              }, index ); // overlapping or

}

addIndexesAndPartitions = function(index) {
    t.ensureIndex(index);
    e = db.getLastErrorObj();
    assert(e.err == null);
    t2.ensureIndex(index);
    e = db.getLastErrorObj();
    assert(e.err == null);
    print("adding partitions");
    for (i = 1; i <= 9; i++) {
        assert.commandWorked(t.addPartition({ts: 10*i}));
    }
}

// test of correctness of SortedPartitionedCursor
runRandomTest = function (index) {
    addIndexesAndPartitions(index);
    print("adding values");
    // now insert 100 values with random values for 'a'
    for (i = 0; i < 100; i++) {
        randVal = Random.randInt( 100 );
        randValb = Random.randInt( 500 );
        t.insert({_id : i, ts: i, a : randVal, b: randValb});
        t2.insert({_id : i, ts: i, a : randVal, b: randValb});
    }
    e = db.getLastErrorObj();
    assert(e.err == null);
    print("run queries");
    runSomeQueries(index);
    // add a partition at end, so now we have an empty partition
    assert.commandWorked(t.addPartition({ts: 100}));
    runSomeQueries(index);
    assert.commandWorked(t.addPartition({ts: 110}));
    assert.commandWorked(t.addPartition({ts: 120}));
    assert.commandWorked(t.addPartition({ts: 130}));
    runSomeQueries(index);
}

runQueryForFilter = function(query, index) {
    //printjson(index);
    explainOutput = t.find(query).hint(index).explain();
    assert.eq("DistributedPartitionedCursor", explainOutput.cursor);
    // for each of these cursors, filtering should make nscanned the same as nscannedobjects
    // this means we need to be careful with queries we pass in
    //printjson(explainOutput);
    assert.eq(explainOutput.nscanned, (t.count(query) + 1));
    //print(t.count(query));
}

runSortedQueryForFilter = function(query, index, expectedNScanned) {
    //printjson(index);
    explainOutput = t.find(query).sort({a:1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // for each of these cursors, filtering should make nscanned the same as nscannedobjects
    // this means we need to be careful with queries we pass in
    //printjson(explainOutput);
    assert.eq(explainOutput.nscanned, expectedNScanned);

    explainOutput = t.find(query).sort({a:-1}).hint(index).explain();
    assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    // for each of these cursors, filtering should make nscanned the same as nscannedobjects
    // this means we need to be careful with queries we pass in
    //printjson(explainOutput);
    assert.eq(explainOutput.nscanned, expectedNScanned);
}


// test of correctness of partition filtering logic
runFilterTest = function (index) {
    addIndexesAndPartitions(index);
    // now insert 100 values with random values for 'a'
    print("adding values for filter test");
    for (i = 0; i <= 9; i++) {
        for (j = 1; j <=10; j++) {
            t.insert({_id : 10*i + j, ts: 10*i + j, a : j, b: j});
        }
    }
    e = db.getLastErrorObj();
    assert(e.err == null);
    print("run queries for filter test");
    runQueryForFilter({$and : [ {a : {$gte: 7}} , {ts : {$gte: 72}} ] }, index);
    runQueryForFilter( {$and : [ {a : {$gte: 5}} , {ts : {$gte: 21}} ] }, index );
    runQueryForFilter( {$and : [ {a : {$gte: 5}} , {ts : {$lte: 79}}, {ts : {$gte : 21}} ] }, index ); // this is a border case
    // these queries do not yield the filtering results we want due to SERVER-5162
    /*
    runQueryForFilter( {$and : [ {$or : [{ts : {$gte: 81}}, {ts : {$lte : 20}}]},{a : {$gte: 5}} ] }, index ); // non-overlapping or
    runQueryForFilter( {$and : [ {a : {$gte: 5}} , {$or : [{ts : {$gte: 35}}, {ts : {$lte : 55}}]} ] }, index ); // overlapping or
    runQueryForFilter( {$and : [ {a : {$gte: 5}} ,
                        {$or : [  {$and : [ {ts : {$gte: 35}}, {ts : {$lte: 55}}]} ,
                                  {$and : [ {ts : {$gte: 45}}, {ts : {$lte: 75}}]}
                               ]
                         }
                      ]
              }, index ); // overlapping or
*/
    // these queries test that the OrRangeGenerator works with multiple $or clauses.
    runSortedQueryForFilter( {$or : [{ts : {$gte: 81}}, {ts : {$lte : 20}}]}, index, 50 ); // non-overlapping or
    runSortedQueryForFilter( {$or : [{ts : {$gte: 35}}, {ts : {$lte : 55}}]}, index, 100 ); // overlapping or
    runSortedQueryForFilter( {$or : [  {$and : [ {ts : {$gte: 35}}, {ts : {$lte: 55}}]} ,
                                  {$and : [ {ts : {$gte: 45}}, {ts : {$lte: 75}}]}
                              ]
                        }, index, 50 ); // overlapping or
}

createColls();
runRandomTest({a:1});
dropColls();
createColls();
runRandomTest({a:-1});
dropColls();
createColls();
runFilterTest({a:1});
dropColls();
createColls();
runFilterTest({a:-1});
dropColls();

