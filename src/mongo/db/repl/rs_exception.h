// @file rs_exception.h

#pragma once

#include "mongo/pch.h"

#include "mongo/util/assert_util.h"

namespace mongo {

    class VoteException : public std::exception {
    public:
        const char * what() const throw () { return "VoteException"; }
    };

    class RetryAfterSleepException : public std::exception {
    public:
        const char * what() const throw () { return "RetryAfterSleepException"; }
    };

    class RollbackOplogException : public DBException {
      public:
        RollbackOplogException() : DBException("Failed to rollback oplog operation", 0) {}
        RollbackOplogException(const string &s) : DBException("Failed to rollback oplog operation: " + s, 0) {}
    };
}
