/**
*    Copyright (C) 2013 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include "mongo/pch.h"

#include <errno.h>

#include <db.h>

#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    namespace storage {

        class Exception : public mongo::DBException {
          public:
            Exception(const mongo::ExceptionInfo &ei) : mongo::DBException(ei) {}
            Exception(const char *msg, int code) : mongo::DBException(msg, code) {}
            Exception(const std::string &msg, int code) : mongo::DBException(msg, code) {}
            virtual ~Exception() throw() {}
        };

        class SystemException : public Exception {
            static mongoutils::str::stream& errprefix(int code) {
                return mongoutils::str::stream() << "Error " << code << "(" << strerror(code) << ") from the ydb layer. ";
            }
          public:
            SystemException(const mongo::ExceptionInfo &ei) : Exception(ei) {}
            SystemException(const char *msg, int err, int code) : Exception(errprefix(err) << msg, code) {}
            SystemException(const std::string &msg, int err, int code) : Exception(errprefix(err) << msg, code) {}
            virtual ~SystemException() throw() {}

            class Enoent;
        };

        class SystemException::Enoent : public SystemException {
          public:
            Enoent() : SystemException("The collection may have been dropped.", ENOENT, 16847) {}
            virtual ~Enoent() throw() {}
        };

        class RetryableException : public Exception {
          public:
            RetryableException(const mongo::ExceptionInfo &ei) : Exception(ei) {}
            RetryableException(const char *msg, int code) : Exception(msg, code) {}
            RetryableException(const std::string &msg, int code) : Exception(msg, code) {}
            virtual ~RetryableException() throw() {}

            class MvccDictionaryTooNew;
        };

        class RetryableException::MvccDictionaryTooNew : public RetryableException {
          public:
            MvccDictionaryTooNew() : RetryableException("Accessed dictionary created after this transaction began.  Try restarting the transaction.", 16768) {}
            virtual ~MvccDictionaryTooNew() throw() {}
        };

        class LockException : public RetryableException {
          public:
            LockException(const mongo::ExceptionInfo &ei) : RetryableException(ei) {}
            LockException(const char *msg, int code) : RetryableException(msg, code) {}
            LockException(const std::string &msg, int code) : RetryableException(msg, code) {}
            virtual ~LockException() throw() {}
        };

        // You really should not catch this.  Something is severely broken and we should crash.
        class DataCorruptionException : public Exception {
          public:
            DataCorruptionException(const mongo::ExceptionInfo &ei) : Exception(ei) {}
            DataCorruptionException(const char *msg, int code) : Exception(mongoutils::str::stream() << msg << " There may be data corruption.", code) {}
            DataCorruptionException(const std::string &msg, int code) : Exception(mongoutils::str::stream() << msg << " There may be data corruption.", code) {}
            virtual ~DataCorruptionException() throw() {}
        };

    }  // namespace storage

}  // namespace mongo
