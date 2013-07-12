// Common helper functions for loader tests.

function begin() {
    db.runCommand({ 'beginTransaction' : 1 });
    e = db.getLastError();
    assert(!e);
}
function commit() {
    db.runCommand({ 'commitTransaction' : 1 });
    e = db.getLastError();
    assert(!e);
}
function rollback() {
    db.runCommand({ 'rollbackTransaction' : 1 });
    e = db.getLastError();
    assert(!e);
}

// begin/commit load wrappers that may or may not assert on failure
function _beginLoad(ns, indexes, options, shouldFail) {
    db.runCommand({ 'beginLoad' : 1, 'ns' : ns, 'indexes' : indexes, 'options' : options });
    e = db.getLastError();
    printjson(e);
    shouldFail ? assert(e) : assert(!e);
}
function _commitLoad(shouldFail) {
    db.runCommand({ 'commitLoad' : 1 });
    e = db.getLastError();
    shouldFail ? assert(e) : assert(!e);
}
function _abortLoad(shouldFail) {
    db.runCommand({ 'abortLoad' : 1 });
    e = db.getLastError();
    shouldFail ? assert(e) : assert(!e);
}
function beginLoad(ns, indexes, options) {
    _beginLoad(ns, indexes, options, false);
}
function beginLoadShouldFail(ns, indexes, options) {
    _beginLoad(ns, indexes, options, true);
}
function commitLoad() {
    _commitLoad(false);
}
function commitLoadShouldFail() {
    _commitLoad(true);
}
function abortLoad() {
    _abortLoad(false);
}
function abortLoadShouldFail() {
    _abortLoad(true);
}

// No indexes, no options.

