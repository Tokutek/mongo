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

#include "mongo/db/lasterror.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    namespace storage {

        /**
           UserException and MsgAssertionException here replicate the hierarchy in assert_util.h,
           but we define a fine-grained hierarchy underneath them so that we can catch some of these
           errors internally as part of the protocol.

           The uasserted and msgasserted functions do some additional stateful stuff like logging
           and getLastError management, so we put that stuff in the constructors here rather than
           write extra wrapper functions.  This means if you do catch and eat one of these
           exceptions, it will already have been logged and have setLastError, so be careful about
           eating them.  So far it's only in roughly user-invisible places like distlock and the
           cloner.

           If you want to reconstruct an already thrown exception, you'll need to do something
           different or risk double-counting errors.
         */
        class UserException : public mongo::UserException {
          public:
            UserException(int code, const std::string &msg) : mongo::UserException(code, msg) {
                assertionCount.condrollover( ++assertionCount.user );
                LOG(1) << "User Assertion: " << code << ":" << msg << endl;
                setLastError(code, msg.c_str());
            }
            virtual ~UserException() throw() {}
        };

        class MsgAssertionException : public mongo::MsgAssertionException {
          public:
            MsgAssertionException(const mongo::ExceptionInfo &ei) : mongo::MsgAssertionException(ei) {}
            MsgAssertionException(int code, const std::string &msg) : mongo::MsgAssertionException(code, msg) {
                assertionCount.condrollover( ++assertionCount.warning );
                log() << "Assertion: " << code << ":" << msg << endl;
                setLastError(code, msg.c_str());
                //breakpoint();
                logContext();
            }
            virtual ~MsgAssertionException() throw() {}
        };

        class SystemException : public MsgAssertionException {
            static std::string errprefix(int code, const std::string& msg) {
                return mongoutils::str::stream() << "Error " << code << " (" << strerror(code) << ") from the ydb layer. " << msg;
            }
          public:
            SystemException(const mongo::ExceptionInfo &ei) : MsgAssertionException(ei) {}
            SystemException(int err, int code, const std::string &msg) : MsgAssertionException(code, errprefix(err, msg)) {}
            virtual ~SystemException() throw() {}

            class Enoent;
        };

        class SystemException::Enoent : public SystemException {
          public:
            Enoent() : SystemException(ENOENT, 16847, "The collection may have been dropped.") {}
            virtual ~Enoent() throw() {}
        };

        class RetryableException : public UserException {
          public:
            RetryableException(int code, const std::string &msg) : UserException(code, msg) {}
            virtual ~RetryableException() throw() {}

            class MvccDictionaryTooNew;
        };

        class RetryableException::MvccDictionaryTooNew : public RetryableException {
          public:
            MvccDictionaryTooNew() : RetryableException(16768, "Accessed dictionary created after this transaction began.  Try restarting the transaction.") {}
            virtual ~MvccDictionaryTooNew() throw() {}
        };

        class LockException : public RetryableException {
          public:
            LockException(int code, const std::string &msg) : RetryableException(code, msg) {}
            virtual ~LockException() throw() {}
        };

        // You really should not catch this.  Something is severely broken and we should crash.
        class DataCorruptionException : public MsgAssertionException {
          public:
            DataCorruptionException(const mongo::ExceptionInfo &ei) : MsgAssertionException(ei) {}
            DataCorruptionException(int code, const std::string &msg) : MsgAssertionException(code, mongoutils::str::stream() << msg << " There may be data corruption.") {}
            virtual ~DataCorruptionException() throw() {}
        };

    }  // namespace storage

}  // namespace mongo
