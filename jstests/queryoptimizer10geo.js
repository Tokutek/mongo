// Optimial indexes are saved in the query plan cache, but not geo indexes. SERVER-5301
t = db.jstests_queryoptimizer10geo;
t.drop();

function clearQueryPlanCache() {
    t.ensureIndex({zzz:1});
    t.dropIndex({zzz:1});
}
function assertIndexRecordedForQuery(expectedCursor, query, explainQuery){
    clearQueryPlanCache();
    explainQuery = explainQuery || query;
    t.find(query).itcount();
    if (!expectedCursor) {
        assert(!t.find(explainQuery).explain(true).oldPlan);
    }
    else {
        assert.eq(expectedCursor, t.find(explainQuery).explain(true).oldPlan.cursor);
    }
}

t.ensureIndex({a:'2d'});
assertIndexRecordedForQuery( null, { a:{ $near:[ 50, 50 ] } } );
assertIndexRecordedForQuery( null, { a:[ 50, 50 ] } )

t.drop();
