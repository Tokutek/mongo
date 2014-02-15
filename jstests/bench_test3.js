t = db.bench_test3


benchArgs = { ops : [ { ns : t.getFullName() ,
                        op : "update" ,
                        upsert : true ,
                        query : { _id : { "#RAND_INT" : [ 0 , 5 , 4 ] } } ,
                        update : { $inc : { x : 1 } } } ] ,
              parallel : 2 ,
              seconds : 1 ,
              totals : true ,
              host : db.getMongo().host }

if (jsTest.options().auth) {
    benchArgs['db'] = 'admin';
    benchArgs['username'] = jsTest.options().adminUser;
    benchArgs['password'] = jsTest.options().adminPassword;
}

for (var i = 0; i < 5; ++i) {
    t.drop();

    res = benchRun( benchArgs )
    printjson( res );

    var keys = []
    var totals = {}
    db.bench_test3.find().sort( { _id : 1 } ).forEach( function(z){ keys.push( z._id ); totals[z._id] = z.x } );

    // Rather than do this assert, since the updates sometimes get lock not granted, we try a few
    // times before failing the test
    // assert.eq( [ 0 , 4 , 8 , 12 , 16 ] , keys )

    if (keys == [ 0 , 4 , 8 , 12 , 16 ]) {
        // pass
        break;
    }

    if (i == 4) {
        assert.eq( [ 0 , 4 , 8 , 12 , 16 ] , keys );
    }
}
