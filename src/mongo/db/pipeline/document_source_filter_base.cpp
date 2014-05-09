/**
*    Copyright (C) 2011 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "pch.h"

#include "db/pipeline/document_source.h"

#include "db/jsobj.h"
#include "db/pipeline/expression.h"
#include "db/pipeline/value.h"

namespace mongo {

    DocumentSourceFilterBase::~DocumentSourceFilterBase() {
    }

    void DocumentSourceFilterBase::findNext() {
        unstarted = false;

        for(bool hasDoc = !pSource->eof(); hasDoc; hasDoc = pSource->advance()) {
            pCurrent = pSource->getCurrent();
            if (accept(pCurrent)) {
                pSource->advance(); // Start next call at correct position
                hasCurrent = true;
                return;
            }
        }

        // Nothing matched
        pCurrent = Document();
        hasCurrent = false;
    }

    bool DocumentSourceFilterBase::eof() {
        if (unstarted)
            findNext();

        return !hasCurrent;
    }

    bool DocumentSourceFilterBase::advance() {
        DocumentSource::advance(); // check for interrupts

        if (unstarted)
            findNext();

        /*
          This looks weird after the above, but is correct.  Note that calling
          getCurrent() when first starting already yields the first document
          in the collection.  Calling advance() without using getCurrent()
          first will skip over the first item.
         */
        findNext();

        return hasCurrent;
    }

    Document DocumentSourceFilterBase::getCurrent() {
        verify(hasCurrent);
        return pCurrent;
    }

    DocumentSourceFilterBase::DocumentSourceFilterBase(
        const intrusive_ptr<ExpressionContext> &pExpCtx):
        DocumentSource(pExpCtx),
        unstarted(true),
        hasCurrent(false),
        pCurrent() {
    }
}
