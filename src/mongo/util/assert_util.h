// assert_util.h

/*    Copyright 2009 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <typeinfo>
#include <string>

#include "mongo/base/status.h" // NOTE: This is safe as utils depend on base
#include "mongo/bson/inline_decls.h"
#include "mongo/platform/compiler.h"
#include "mongo/util/debug_util.h"

namespace mongo {

    enum CommonErrorCodes {
        OkCode = 0,
        SendStaleConfigCode = 13388 ,     // uassert( 13388 )
        RecvStaleConfigCode = 9996,       // uassert( 9996 )
        PrepareConfigsFailedCode = 13104  // uassert( 13104 )
    };

    class AssertionCount {
    public:
        AssertionCount();
        void rollover();
        void condrollover( int newValue );

        int regular;
        int warning;
        int msg;
        int user;
        int rollovers;
    };

    extern AssertionCount assertionCount;

    class BSONObjBuilder;

    struct ExceptionInfo {
        ExceptionInfo() : msg(""),code(-1) {}
        ExceptionInfo( const char * m , int c )
            : msg( m ) , code( c ) {
        }
        ExceptionInfo( const std::string& m , int c )
            : msg( m ) , code( c ) {
        }
        void append( BSONObjBuilder& b , const char * m = "$err" , const char * c = "code" ) const ;
        std::string toString() const;
        bool empty() const { return msg.empty(); }        
        void reset(){ msg = ""; code=-1; }
        std::string msg;
        int code;
    };

    /** helper class that builds error strings.  lighter weight than a StringBuilder, albeit less flexible.
        NOINLINE_DECL used in the constructor implementations as we are assuming this is a cold code path when used.

        example: 
          throw UserException(123, ErrorMsg("blah", num_val));
    */
    class ErrorMsg { 
    public:
        ErrorMsg(const char *msg, char ch);
        ErrorMsg(const char *msg, unsigned val);
        operator std::string() const { return buf; }
    private:
        char buf[256];
    };

    class DBException;
    std::string causedBy( const DBException& e );
    std::string causedBy( const std::string& e );
    bool inShutdown();

    /** Most mongo exceptions inherit from this; this is commonly caught in most threads */
    class DBException : public std::exception {
    public:
        DBException( const ExceptionInfo& ei ) : _ei(ei) { traceIfNeeded(*this); }
        DBException( const char * msg , int code ) : _ei(msg,code) { traceIfNeeded(*this); }
        DBException( const std::string& msg , int code ) : _ei(msg,code) { traceIfNeeded(*this); }
        virtual ~DBException() throw() { }

        virtual const char* what() const throw() { return _ei.msg.c_str(); }
        virtual int getCode() const { return _ei.code; }
        virtual void appendPrefix( std::stringstream& ss ) const { }
        virtual void addContext( const std::string& str ) {
            _ei.msg = str + causedBy( _ei.msg );
        }

        // Utilities for the migration to Status objects
        static ErrorCodes::Error convertExceptionCode(int exCode);

        Status toStatus(const std::string& context) const {
            return Status(convertExceptionCode(getCode()), context + causedBy(*this));
        }
        Status toStatus() const {
            return Status(convertExceptionCode(getCode()), this->toString());
        }

        // context when applicable. otherwise ""
        std::string _shard;

        virtual std::string toString() const;

        const ExceptionInfo& getInfo() const { return _ei; }
    private:
        static void traceIfNeeded( const DBException& e );
    public:
        static bool traceExceptions;

    protected:
        ExceptionInfo _ei;
    };

    class AssertionException : public DBException {
    public:

        AssertionException( const ExceptionInfo& ei ) : DBException(ei) {}
        AssertionException( const char * msg , int code ) : DBException(msg,code) {}
        AssertionException( const std::string& msg , int code ) : DBException(msg,code) {}

        virtual ~AssertionException() throw() { }

        virtual bool severe() { return true; }
        virtual bool isUserAssertion() { return false; }

        /* true if an interrupted exception - see KillCurrentOp */
        bool interrupted() {
            return _ei.code == 11600 || _ei.code == 11601;
        }
    };

    /* UserExceptions are valid errors that a user can cause, like out of disk space or duplicate key */
    class UserException : public AssertionException {
    public:
        UserException(int c , const std::string& m) : AssertionException( m , c ) {}
        virtual bool severe() { return false; }
        virtual bool isUserAssertion() { return true; }
        virtual void appendPrefix( std::stringstream& ss ) const;
    };

    class MsgAssertionException : public AssertionException {
    public:
        MsgAssertionException( const ExceptionInfo& ei ) : AssertionException( ei ) {}
        MsgAssertionException(int c, const std::string& m) : AssertionException( m , c ) {}
        virtual bool severe() { return false; }
        virtual void appendPrefix( std::stringstream& ss ) const;
    };

    /** ExceptionSaver can be subclassed to create an object suitable for use in a C callback.
        Use of this class allows C callbacks to throw exceptions around a C API that is not internally exception-safe.
        Typical use is something like this:

            class ExtraArg : public ExceptionSaver {
              public:
                int foo;
            };

            int cb(int x, void *extra) {
                ExtraArg *e = static_cast<ExtraArg *>(extra);
                try {
                    e->foo = bar(x);
                    return 0;
                }
                catch (const std::exception &ex) {
                    e->saveException(ex);
                    return -1;
                }
            }

            void baz() {
                ExtraArg extra;
                int r = c_api_func(42, &extra);
                if (r == -1) {
                    extra.throwException();
                }
            }

        It tries several dynamic_casts at save time in order to record the static type of the exception.
        Therefore, the thrown exception will have the same static type (as long as it is one of the listed types).
    */
    class ExceptionSaver {
        boost::scoped_ptr<MsgAssertionException> _mae;
        boost::scoped_ptr<UserException> _ue;
        boost::scoped_ptr<AssertionException> _ae;
        boost::scoped_ptr<DBException> _dbe;
        boost::scoped_ptr<std::exception> _e;
      public:
        void saveException(const std::exception &e) {
            const MsgAssertionException *mae = dynamic_cast<const MsgAssertionException *>(&e);
            if (mae) {
                _mae.reset(new MsgAssertionException(*mae));
                return;
            }
            const UserException *ue = dynamic_cast<const UserException *>(&e);
            if (ue) {
                _ue.reset(new UserException(*ue));
                return;
            }
            const AssertionException *ae = dynamic_cast<const AssertionException *>(&e);
            if (ae) {
                _ae.reset(new AssertionException(*ae));
                return;
            }
            const DBException *dbe = dynamic_cast<const DBException *>(&e);
            if (dbe) {
                _dbe.reset(new DBException(*dbe));
                return;
            }
            _e.reset(new std::exception(e));
        }
        void throwException() const {
            if (_mae) {
                throw *_mae;
            }
            if (_ue) {
                throw *_ue;
            }
            if (_ae) {
                throw *_ae;
            }
            if (_dbe) {
                throw *_dbe;
            }
            if (_e) {
                throw *_e;
            }
        }
    };

    MONGO_COMPILER_NORETURN void verifyFailed(const char *msg, const char *file, unsigned line);
    void wasserted(const char *msg, const char *file, unsigned line);
    MONGO_COMPILER_NORETURN void fassertFailed( int msgid );
    
    /** a "user assertion".  throws UserAssertion.  logs.  typically used for errors that a user
        could cause, such as duplicate key, disk full, etc.
    */
    MONGO_COMPILER_NORETURN void uasserted(int msgid, const char *msg);
    MONGO_COMPILER_NORETURN void uasserted(int msgid , const std::string &msg);

    /** msgassert and massert are for errors that are internal but have a well defined error text std::string.
        a stack trace is logged.
    */
    MONGO_COMPILER_NORETURN void msgassertedNoTrace(int msgid, const char *msg);
    MONGO_COMPILER_NORETURN inline void msgassertedNoTrace(int msgid, const std::string& msg) {
        msgassertedNoTrace( msgid , msg.c_str() );
    }
    MONGO_COMPILER_NORETURN void msgasserted(int msgid, const char *msg);
    MONGO_COMPILER_NORETURN void msgasserted(int msgid, const std::string &msg);

    /* convert various types of exceptions to strings */
    inline std::string causedBy( const char* e ){ return (std::string)" :: caused by :: " + e; }
    inline std::string causedBy( const DBException& e ){ return causedBy( e.toString().c_str() ); }
    inline std::string causedBy( const std::exception& e ){ return causedBy( e.what() ); }
    inline std::string causedBy( const std::string& e ){ return causedBy( e.c_str() ); }
    inline std::string causedBy( const std::string* e ){
        return (e && *e != "") ? causedBy(*e) : "";
    }
    inline std::string causedBy( const Status& e ){ return causedBy( e.reason() ); }

    /** aborts on condition failure */
    inline void fassert(int msgid, bool testOK) {if (MONGO_unlikely(!testOK)) fassertFailed(msgid);}


    /* "user assert".  if asserts, user did something wrong, not our code */
#define MONGO_uassert(msgid, msg, expr) (void)( MONGO_likely(!!(expr)) || (mongo::uasserted(msgid, msg), 0) )

#define MONGO_uassertStatusOK(expr) do {                                  \
        Status status = (expr);                                         \
        if (!status.isOK())                                             \
            uasserted(status.location() != 0 ? status.location() : status.code(), \
                      status.reason());                                 \
    } while(0)

    /* warning only - keeps going */
#define MONGO_wassert(_Expression) (void)( MONGO_likely(!!(_Expression)) || (mongo::wasserted(#_Expression, __FILE__, __LINE__), 0) )
#define MONGO_wunimplemented(msg) MONGO_RARELY { problem() << "tokumx unimplemented " << msg << " " << __FILE__ << ":" << __LINE__ << endl; }

    /* display a message, no context, and throw assertionexception

       easy way to throw an exception and log something without our stack trace
       display happening.
    */
#define MONGO_massert(msgid, msg, expr) (void)( MONGO_likely(!!(expr)) || (mongo::msgasserted(msgid, msg), 0) )
    /* same as massert except no msgid */
#define MONGO_verify(_Expression) (void)( MONGO_likely(!!(_Expression)) || (mongo::verifyFailed(#_Expression, __FILE__, __LINE__), 0) )
#define MONGO_unimplemented(msg) (void)(mongo::verifyFailed("tokumx unimplemented " msg, __FILE__, __LINE__), 0)

    /* dassert is 'debug assert' -- might want to turn off for production as these
       could be slow.
    */
#if defined(_DEBUG)
# define MONGO_dassert(x) fassert(16199, (x))
#else
# define MONGO_dassert(x)
#endif

    /** Allows to jump code during exeuction. */
    inline bool debugCompare(bool inDebug, bool condition) { return inDebug && condition; }

#if defined(_DEBUG)
# define MONGO_debug_and(x) debugCompare(true, (x))
#else
# define MONGO_debug_and(x) debugCompare(false, (x))
#endif

#ifdef MONGO_EXPOSE_MACROS
# define dcompare MONGO_debug_and
# define dassert MONGO_dassert
# define verify MONGO_verify
# define uassert MONGO_uassert
# define uassertStatusOK MONGO_uassertStatusOK
# define wassert MONGO_wassert
# define wunimplemented MONGO_wunimplemented
# define massert MONGO_massert
# define unimplemented MONGO_unimplemented
#endif

    // some special ids that we want to duplicate

    // > 10000 asserts
    // < 10000 UserException

    enum {
        ASSERT_ID_LOCK_DEADLOCK = 9998,
        ASSERT_ID_LOCK_NOTGRANTED = 9999,
        ASSERT_ID_DUPKEY = 11000,
    };

    /* throws a uassertion with an appropriate msg */
    MONGO_COMPILER_NORETURN void streamNotGood( int code, const std::string& msg, std::ios& myios );

    inline void assertStreamGood(unsigned msgid, const std::string& msg, std::ios& myios) {
        if( !myios.good() ) streamNotGood(msgid, msg, myios);
    }

    std::string demangleName( const std::type_info& typeinfo );

} // namespace mongo

#define MONGO_ASSERT_ON_EXCEPTION( expression ) \
    try { \
        expression; \
    } catch ( const std::exception &e ) { \
        stringstream ss; \
        ss << "caught exception: " << e.what() << ' ' << __FILE__ << ' ' << __LINE__; \
        msgasserted( 13294 , ss.str() ); \
    } catch ( ... ) { \
        massert( 10437 ,  "unknown exception" , false ); \
    }

#define MONGO_ASSERT_ON_EXCEPTION_WITH_MSG( expression, msg ) \
    try { \
        expression; \
    } catch ( const std::exception &e ) { \
        stringstream ss; \
        ss << msg << " caught exception exception: " << e.what();   \
        msgasserted( 14043 , ss.str() );        \
    } catch ( ... ) { \
        msgasserted( 14044 , std::string("unknown exception") + msg );   \
    }

#define DESTRUCTOR_GUARD MONGO_DESTRUCTOR_GUARD
#define MONGO_DESTRUCTOR_GUARD( expression ) \
    try { \
        expression; \
    } catch ( const std::exception &e ) { \
        problem() << "caught exception (" << e.what() << ") in destructor (" << __FUNCTION__ << ")" << endl; \
    } catch ( ... ) { \
        problem() << "caught unknown exception in destructor (" << __FUNCTION__ << ")" << endl; \
    }

