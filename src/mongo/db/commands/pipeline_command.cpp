/**
 * Copyright (c) 2011 10gen Inc.
 * Copyright (C) 2013 Tokutek Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "mongo/pch.h"

#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/command_cursors.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/interrupt_status_mongod.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/ops/query.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    extern const int MaxBytesToReturnToClientAtOnce;

    class PipelineCursor : public Cursor {
    public:
        PipelineCursor(intrusive_ptr<Pipeline> pipeline)
            : _pipeline(pipeline)
        {}

        // "core" cursor protocol
        virtual bool ok() { return !iterator()->eof(); }
        virtual bool advance() { return iterator()->advance(); }
        virtual BSONObj current() {
            BSONObjBuilder builder;
            iterator()->getCurrent().toBson(&builder);
            return builder.obj();
        }

        virtual bool shouldDestroyOnNSDeletion() { return false; }

        virtual bool getsetdup(const BSONObj &pk) { return false; } // we don't generate dups
        virtual bool isMultiKey() const { return false; }
        virtual bool modifiedKeys() const { return false; }
        virtual string toString() const { return "Aggregate_Cursor"; }

        // These probably won't be needed once aggregation supports it's own explain.
        virtual long long nscanned() const { return 0; }
        virtual void explainDetails( BSONObjBuilder& b ) const { return; }
    private:
        const DocumentSource* iterator() const { return _pipeline->output(); }
        DocumentSource* iterator() { return _pipeline->output(); }

        intrusive_ptr<Pipeline> _pipeline;
    };

    class PipelineCommand :
        public Command {
    public:
        // virtuals from Command
        virtual ~PipelineCommand();
        virtual bool run(const string &db, BSONObj &cmdObj, int options,
                         string &errmsg, BSONObjBuilder &result, bool fromRepl);
        virtual LockType locktype() const;
        // can't know if you're going to do a write op yet, you just shouldn't do aggregate on a SyncClusterConnection
        virtual bool requiresSync() const { return true; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings().setBulkFetch(true); }
        virtual bool slaveOk() const;
        // aggregate is like a query, we don't need to hold this lock
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual void help(stringstream &help) const;
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out);

        PipelineCommand();

    private:
        /*
          For the case of explain, we don't want to hold any lock at all,
          because it generates warnings about recursive locks.  However,
          the getting the explain information for the underlying cursor uses
          the direct client cursor, and that gets a lock.  Therefore, we need
          to take steps to avoid holding a lock while we use that.  On the
          other hand, we need to have a READ lock for normal explain execution.
          Therefore, the lock is managed manually, and not through the virtual
          locktype() above.

          In order to achieve this, locktype() returns NONE, but the lock that
          would be managed for reading (for executing the pipeline in the
          regular way),  will be managed manually here.  This code came from
          dbcommands.cpp, where objects are constructed to hold the lock
          and automatically release it on destruction.  The use of this
          pattern requires extra functions to hold the lock scope and from
          within which to execute the other steps of the explain.

          The arguments for these are all the same, and come from run(), but
          are passed in so that new blocks can be created to hold the
          automatic locking objects.
         */

        /*
          Execute the pipeline for the explain.  This is common to both the
          locked and unlocked code path.  However, the results are different.
          For an explain, with no lock, it really outputs the pipeline
          chain rather than fetching the data.
         */
        bool executeSplitPipeline(
            BSONObjBuilder& result, string& errmsg, const string& ns, const string& db,
            intrusive_ptr<Pipeline>& pPipeline,
            intrusive_ptr<ExpressionContext>& pCtx);
    };

    // self-registering singleton static instance
    static PipelineCommand pipelineCommand;

    PipelineCommand::PipelineCommand():
        Command(Pipeline::commandName) {
    }

    Command::LockType PipelineCommand::locktype() const {
        // Locks are managed manually, in particular by DocumentSourceCursor.
        return OPLOCK;
    }

    bool PipelineCommand::slaveOk() const {
        return true;
    }

    void PipelineCommand::help(stringstream &help) const {
        help << "{ pipeline : [ { <data-pipe-op>: {...}}, ... ] }";
    }

    void PipelineCommand::addRequiredPrivileges(const std::string& dbname,
                                                const BSONObj& cmdObj,
                                                std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::find);
        out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
    }

    PipelineCommand::~PipelineCommand() {
    }

    bool PipelineCommand::executeSplitPipeline(
            BSONObjBuilder& result, string& errmsg,
            const string& ns, const string& db,
            intrusive_ptr<Pipeline>& pPipeline,
            intrusive_ptr<ExpressionContext>& pCtx) {

        /* setup as if we're in the router */
        pCtx->setInRouter(true);

        /*
          Here, we'll split the pipeline in the same way we would for sharding,
          for testing purposes.

          Run the shard pipeline first, then feed the results into the remains
          of the existing pipeline.

          Start by splitting the pipeline.
         */
        intrusive_ptr<Pipeline> pShardSplit(
            pPipeline->splitForSharded());

        /*
          Write the split pipeline as we would in order to transmit it to
          the shard servers.
        */
        BSONObjBuilder shardBuilder;
        pShardSplit->toBson(&shardBuilder);
        BSONObj shardBson(shardBuilder.done());

        DEV (log() << "\n---- shardBson\n" <<
             shardBson.jsonString(Strict, 1) << "\n----\n");

        /* for debugging purposes, show what the pipeline now looks like */
        DEV {
            BSONObjBuilder pipelineBuilder;
            pPipeline->toBson(&pipelineBuilder);
            BSONObj pipelineBson(pipelineBuilder.done());
            (log() << "\n---- pipelineBson\n" <<
             pipelineBson.jsonString(Strict, 1) << "\n----\n");
        }

        /* on the shard servers, create the local pipeline */
        intrusive_ptr<ExpressionContext> pShardCtx(
            ExpressionContext::create(&InterruptStatusMongod::status));
        intrusive_ptr<Pipeline> pShardPipeline(
            Pipeline::parseCommand(errmsg, shardBson, pShardCtx));
        if (!pShardPipeline.get()) {
            return false;
        }

        PipelineD::prepareCursorSource(pShardPipeline, nsToDatabase(ns), pCtx);

        /* run the shard pipeline */
        BSONObjBuilder shardResultBuilder;
        string shardErrmsg;
        pShardPipeline->stitch();
        pShardPipeline->run(shardResultBuilder);
        BSONObj shardResult(shardResultBuilder.done());

        /* pick out the shard result, and prepare to read it */
        intrusive_ptr<DocumentSourceBsonArray> pShardSource;
        BSONObjIterator shardIter(shardResult);
        while(shardIter.more()) {
            BSONElement shardElement(shardIter.next());
            const char *pFieldName = shardElement.fieldName();

            if ((strcmp(pFieldName, "result") == 0) ||
                (strcmp(pFieldName, "serverPipeline") == 0)) {
                pPipeline->addInitialSource(DocumentSourceBsonArray::create(&shardElement, pCtx));
                pPipeline->stitch();

                /*
                  Connect the output of the shard pipeline with the mongos
                  pipeline that will merge the results.
                */
                pPipeline->run(result);
                return true;
            }
        }

        /* NOTREACHED */
        verify(false);
        return false;
    }

    bool PipelineCommand::run(const string &db, BSONObj &cmdObj,
                              int options, string &errmsg,
                              BSONObjBuilder &result, bool fromRepl) {

        intrusive_ptr<ExpressionContext> pCtx(
            ExpressionContext::create(&InterruptStatusMongod::status));

        /* try to parse the command; if this fails, then we didn't run */
        intrusive_ptr<Pipeline> pPipeline(
            Pipeline::parseCommand(errmsg, cmdObj, pCtx));
        if (!pPipeline.get())
            return false;

        string ns(parseNs(db, cmdObj));

        if (pPipeline->getSplitMongodPipeline()) {
            // This is only used in testing
            return executeSplitPipeline(result, errmsg, ns, db, pPipeline, pCtx);
        }

#if _DEBUG
        // This is outside of the if block to keep the object alive until the pipeline is finished.
        BSONObj parsed;
        if (!pPipeline->isExplain() && !pCtx->getInShard()) {
            // Make sure all operations round-trip through Pipeline::toBson()
            // correctly by reparsing every command on DEBUG builds. This is
            // important because sharded aggregations rely on this ability.
            // Skipping when inShard because this has already been through the
            // transformation (and this unsets pCtx->inShard).
            BSONObjBuilder bb;
            pPipeline->toBson(&bb);
            parsed = bb.obj();
            pPipeline = Pipeline::parseCommand(errmsg, parsed, pCtx);
            verify(pPipeline);
        }
#endif

        // This does the mongod-specific stuff like creating a cursor
        PipelineD::prepareCursorSource(pPipeline, nsToDatabase(ns), pCtx);
        pPipeline->stitch();

        if (isCursorCommand(cmdObj)) {
            uasserted(17364, "aggregation cursor isn't yet supported");
            CursorId id;
            {
                // Set up cursor
                LOCK_REASON(lockReason, "aggregate: creating aggregation cursor");
                Client::ReadContext ctx(ns, lockReason);
                shared_ptr<Cursor> cursor(new PipelineCursor(pPipeline));
                // cc will be owned by cursor manager
                ClientCursor* cc = new ClientCursor(0, cursor, ns, cmdObj.getOwned());
                id = cc->cursorid();
            }

            handleCursorCommand(id, cmdObj, result);
        }
        else {
            pPipeline->run(result);
        }

        return true;
    }

} // namespace mongo
