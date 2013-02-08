/**
*    Copyright (C) 2012 Tokutek Inc.
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

#include "env.h"

#include "mongo/pch.h"

#include <db.h>
#include <boost/filesystem.hpp>
#ifdef _WIN32
# error "Doesn't support windows."
#endif
#include <fcntl.h>

#include "mongo/util/log.h"

namespace mongo {

    // TODO: Should be in CmdLine or something.
    extern string dbpath;

    namespace storage {

        DB_ENV *env;

        void startup(void) {
            tokulog() << "startup" << endl;

            const int env_flags = DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_INIT_LOG|DB_RECOVER;
            const int env_mode = S_IRWXU|S_IRGRP|S_IROTH|S_IXGRP|S_IXOTH;
            boost::filesystem::path envdir(dbpath);
            envdir /= "storage";

            boost::filesystem::create_directory(envdir);
            int r = db_env_create(&env, 0);
            verify(r == 0);
            r = env->open(env, envdir.string().c_str(), env_flags, env_mode);
            verify(r == 0);
        }

        void shutdown(void) {
            tokulog() << "shutdown" << endl;

            int r = env->close(env, 0);
            verify(r == 0);
        }

        DB *db_open(DB_TXN *txn, const char *name) {
            tokulog() << "opening db " << name << endl;
            DB *db;
            int r = db_create(&db, env, 0);
            verify(r == 0);
            const int db_flags = DB_CREATE;
            r = db->open(db, txn, name, NULL, DB_BTREE, db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            verify(r == 0);
            return db;

        }

        void db_close(DB *db) {
            tokulog() << "closing db" << endl;
            int r = db->close(db, 0);
            verify(r == 0);
        }

    } // namespace storage

} // namespace mongo
