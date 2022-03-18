/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;

public class AsyncFilteredRecordCursorFactory implements RecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final AsyncFilteredRecordCursor cursor;
    private final Function filter;
    private final PageFrameSequence<Function> frameSequence;
    private final SCSequence collectSubSeq = new SCSequence();

    public AsyncFilteredRecordCursorFactory(
            CairoConfiguration configuration,
            MessageBus messageBus,
            RecordCursorFactory base,
            Function filter
    ) {
        assert !(base instanceof AsyncFilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new AsyncFilteredRecordCursor(filter, base.hasDescendingOrder());
        this.filter = filter;
        this.frameSequence = new PageFrameSequence<>(configuration, messageBus, REDUCER);
    }

    @Override
    public void close() {
        Misc.free(base);
        Misc.free(filter);
        Misc.free(frameSequence);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(
                collectSubSeq,
                execute(executionContext, collectSubSeq)
        );
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public PageFrameSequence<Function> execute(SqlExecutionContext executionContext, Sequence collectSubSeq) throws SqlException {
        return frameSequence.dispatch(base, executionContext, collectSubSeq, filter);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean hasDescendingOrder() {
        return base.hasDescendingOrder();
    }

    private static void filter(PageAddressCacheRecord record, PageFrameReduceTask task) {
        final DirectLongList rows = task.getRows();
        final long frameRowCount = task.getFrameRowCount();
        final Function filter = task.getFrameSequence(Function.class).getAtom();

        rows.clear();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }
}
