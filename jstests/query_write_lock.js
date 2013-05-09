// verify that queries that need a write lock work
// force mongod to run a query on a closed collection

db.rfp.drop()
db.rfp.insert({a:1})
cursor=db.rfp.find()
count=0
cursor.forEach(function(doc) { count++; })
assert.eq(1,count, "rows")

// this closes the rfp collection
db.runCommand("beginTransaction")
db.rfp.renameCollection("rfp2")
db.runCommand("rollbackTransaction")

// a count command is not handled by runQuery
// db.rfp.count()
// db.rfp.find().count()

// the find runs a query on the rfp collection
// mongod has to open it
cursor=db.rfp.find()
count=0
cursor.forEach(function(doc) { count++; })
assert.eq(1,count, "rows")





