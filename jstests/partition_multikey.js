// test that renaming a partitioned collection works as expected
Random.setRandomSeed();
tn = "partition_sorted_cursor";
t = db[tn];
t.drop();

createColls = function () {
    assert.commandWorked(db.createCollection(tn, {partitioned:1, primaryKey : {ts:1, _id:1}}));
}

dropColls = function() {
    t.drop();
}


runQuery = function(query, index, isMulti, isSortedCursor) {
    //printjson(index);
    printjson(query);
    printjson(index);
    explainOutput = t.find(query).sort({a:1}).hint(index).explain();
    printjson(explainOutput);
    if (isSortedCursor) {
        assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    }
    else {
        assert.eq("DistributedPartitionedCursor", explainOutput.cursor);
    }
    assert.eq(isMulti, explainOutput.isMultiKey)

    explainOutput = t.find(query).sort({a:-1}).hint(index).explain();
    if (isSortedCursor) {
        assert.eq("SortedPartitionedCursor", explainOutput.cursor);
    }
    else {
        assert.eq("DistributedPartitionedCursor", explainOutput.cursor);
    }
    assert.eq(isMulti, explainOutput.isMultiKey)

    explainOutput = t.find(query).hint(index).explain();
    assert.eq("DistributedPartitionedCursor", explainOutput.cursor);
    assert.eq(isMulti, explainOutput.isMultiKey)
}

createData = function(index) {
    t.ensureIndex(index);
    e = db.getLastErrorObj();
    assert(e.err == null);
    print("adding partitions\n");
    for (i = 1; i <= 9; i++) {
        assert.commandWorked(t.addPartition({ts: 10*i}));
    }
    print("adding values\n");
    // now insert 100 values with random values for 'a'
    for (i = 0; i < 100; i++) {
        randVal = Random.randInt( 100 );
        t.insert({_id : i, ts: i, a : randVal});
    }
    e = db.getLastErrorObj();
    assert(e.err == null);
    
    runQuery({}, index, false, true);
    runQuery({a : {$gte: 25}}, index, false, true);
    runQuery({a : {$lte: 75}}, index, false, true);
    runQuery( {$and : [ {a : {$gte: 25}} , {a : {$lte: 75}} ] }, index, false , true);
    runQuery({a : {$gte: 1000}}, index, false, true); // query with no results
    // query with filter
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 75}} ] }, index, false, true );
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 5}} ] }, index, false, false );

    // now make the index multikey
    t.insert({_id : 100, ts: 100, a : [1,2]});
    runQuery({}, index, true, true);
    runQuery({a : {$gte: 25}}, index, true, true);
    runQuery({a : {$lte: 75}}, index, true, true);
    runQuery( {$and : [ {a : {$gte: 25}} , {a : {$lte: 75}} ] }, index, true, true );
    runQuery({a : {$gte: 1000}}, index, true, true); // query with no results
    // query with filter
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 75}} ] }, index, true, true );
    runQuery( {$and : [ {a : {$gte: 25}} , {ts : {$lte: 5}} ] }, index, true, false );

}

createColls();
createData({a:1});
dropColls();
createColls();
createData({a:-1});
dropColls();


