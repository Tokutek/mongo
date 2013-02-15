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

#include <string>

#include <db.h>
#include <toku_os.h>
#include <boost/filesystem.hpp>
#ifdef _WIN32
# error "Doesn't support windows."
#endif
#include <fcntl.h>

#include "mongo/db/client.h"
#include "mongo/util/log.h"

namespace mongo {

    // TODO: Should be in CmdLine or something.
    extern string dbpath;

    namespace storage {

        DB_ENV *env;

        static int dbt_bson_compare(DB *db, const DBT *key1, const DBT *key2) {
            // extract bson objects from each dbt and get the ordering
            verify(db->cmp_descriptor);
            const DBT *key_pattern_dbt = &db->cmp_descriptor->dbt;
            const BSONObj key_pattern(static_cast<char *>(key_pattern_dbt->data));
            const Ordering ordering = Ordering::make(key_pattern);

            // Primary _id key is represented by one BSON Object.
            // Secondary keys are represented by two, the secondary key plus _id key.
            dassert(key1->size > 0);
            dassert(key2->size > 0);
            const BSONObj obj1(static_cast<char *>(key1->data));
            const BSONObj obj2(static_cast<char *>(key2->data));
            dassert((int) key1->size >= obj1.objsize());
            dassert((int) key2->size >= obj2.objsize());

            // Compare by the first object. If they are equal and there
            // is another object after the first, compare by the second.
            int c = obj1.woCompare(obj2, ordering);
            if (c < 0) {
                return -1;
            } else if (c > 0) {
                return 1;
            }

            int key1_bytes_left = key1->size - obj1.objsize();
            int key2_bytes_left = key2->size - obj2.objsize();
            if (key1_bytes_left > 0 && key2_bytes_left > 0) {
                // Equal first keys, and there is a second key that comes after.
                const BSONObj other_obj1(static_cast<char *>(key1->data) + obj1.objsize());
                const BSONObj other_obj2(static_cast<char *>(key2->data) + obj2.objsize());
                dassert(obj1.objsize() + other_obj1.objsize() == (int) key1->size);
                dassert(obj2.objsize() + other_obj2.objsize() == (int) key2->size);
                c = other_obj1.woCompare(other_obj2, ordering);
                if (c < 0) {
                    return -1;
                } else if (c > 0) {
                    return 1;
                }
                return 0;
            } else if (key1_bytes_left > 0 && key2_bytes_left == 0) {
                // key 1 has bytes left, but key 2 does not.
                return 1;
            } else if (key1_bytes_left == 0 && key2_bytes_left > 0) {
                // key 1 has no bytes left, but key 2 does.
                return -1;
            } else { // no second key after the first object, so key1 == key2
                return 0;
            }
        }

        static uint64_t calculate_cachesize(void) {
            uint64_t physmem, maxdata;
            physmem = toku_os_get_phys_memory_size();
            uint64_t cache_size = physmem / 2;
            int r = toku_os_get_max_process_data_size(&maxdata);
            if (r == 0) {
                if (cache_size > maxdata / 8) {
                    cache_size = maxdata / 8;
                }
            }
            return cache_size;
        }

        void startup(void) {
            tokulog() << "startup" << endl;
            boost::filesystem::path envdir(dbpath);
            envdir /= "storage";
            boost::filesystem::create_directory(envdir);
            int r = db_env_create(&env, 0);
            verify(r == 0);

            const uint64_t cachesize = calculate_cachesize();
            const uint32_t bytes = cachesize % (1024L * 1024L * 1024L);
            const uint32_t gigabytes = cachesize >> 30;
            r = env->set_cachesize(env, gigabytes, bytes, 1);
            verify(r == 0);
            tokulog() << "cachesize set to " << gigabytes << " GB + " << bytes << " bytes."<< endl;

            r = env->set_default_bt_compare(env, dbt_bson_compare);
            verify(r == 0);

            const int env_flags = DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_INIT_LOG|DB_RECOVER;
            const int env_mode = S_IRWXU|S_IRGRP|S_IROTH|S_IXGRP|S_IXOTH;
            r = env->open(env, envdir.string().c_str(), env_flags, env_mode);
            verify(r == 0);

            const int checkpoint_period = 60;
            r = env->checkpointing_set_period(env, checkpoint_period);
            verify(r == 0);
            tokulog() << "checkpoint period set to " << checkpoint_period << " seconds." << endl;

            const int cleaner_period = 2;
            r = env->cleaner_set_period(env, cleaner_period);
            verify(r == 0);
            tokulog() << "cleaner period set to " << cleaner_period << " seconds." << endl;

            const int cleaner_iterations = 5;
            r = env->cleaner_set_iterations(env, cleaner_iterations);
            verify(r == 0);
            tokulog() << "cleaner iterations set to " << cleaner_iterations << "." << endl;
        }

        void shutdown(void) {
            tokulog() << "shutdown" << endl;
            int r = env->close(env, 0);
            verify(r == 0);
        }

        // set a descriptor for the given dictionary. the descriptor is
        // a serialization of the index's key pattern.
        static void set_db_descriptor(DB *db, DB_TXN *txn, const BSONObj &key_pattern) {
            DBT ordering_dbt;
            ordering_dbt.data = const_cast<char *>(key_pattern.objdata());
            ordering_dbt.size = key_pattern.objsize();
            const int flags = DB_UPDATE_CMP_DESCRIPTOR;
            int r = db->change_descriptor(db, txn, &ordering_dbt, flags);
            verify(r == 0);
            tokulog() << "set db " << db << " descriptor to key pattern: " << key_pattern << endl;
        }

        DB *db_open(const string &name, const BSONObj &key_pattern, bool may_create) {
            const Client::Transaction &txn = cc().transaction();
            dassert(txn.is_root());

            DB *db;
            int r = db_create(&db, env, 0);
            verify(r == 0);

            const int db_flags = may_create ? DB_CREATE : 0;
            r = db->open(db, txn.txn(), name.c_str(), NULL, DB_BTREE, db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            verify(r == 0);

            set_db_descriptor(db, txn.txn(), key_pattern);
            return db;
        }

        void db_close(DB *db) {
            int r = db->close(db, 0);
            verify(r == 0);
        }

    } // namespace storage

} // namespace mongo
