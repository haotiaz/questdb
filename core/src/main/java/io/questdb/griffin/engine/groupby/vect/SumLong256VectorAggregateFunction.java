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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.griffin.SqlCodeGenerator.GKK_HOUR_INT;

public class SumLong256VectorAggregateFunction extends Long256Function implements VectorAggregateFunction {
    private final AtomicBoolean lock = new AtomicBoolean();
    private final Long256Impl sum = new Long256Impl();
    private final LongAdder count = new LongAdder();
    private final int columnIndex;
    private final DistinctFunc distinctFunc;
    private final KeyValueFunc keyValueFunc;
    private int valueOffset;

    public SumLong256VectorAggregateFunction(int keyKind, int columnIndex, int workerCount) {
        this.columnIndex = columnIndex;
        if (keyKind == GKK_HOUR_INT) {
            distinctFunc = Rosti::keyedHourDistinct;
            keyValueFunc = Rosti::keyedHourSumLong;
        } else {
            distinctFunc = Rosti::keyedIntDistinct;
            keyValueFunc = Rosti::keyedIntSumLong;
        }
    }

    @Override
    public void aggregate(long address, long addressSize, int columnSizeHint, int workerId) {
        if (address != 0) {
            final long count = addressSize / (Long.BYTES * 4);
            Long256Impl value = new Long256Impl();
            long offset = 0;
            for (long i = 0; i < count; i++) {
                final long l0 = Unsafe.getUnsafe().getLong(address + offset);
                final long l1 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES);
                final long l2 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES * 2);
                final long l3 = Unsafe.getUnsafe().getLong(address + offset + Long.BYTES * 3);
                value.add(l0, l1, l2, l3);
                offset += 4 * Long.BYTES;
            }
            if (!value.equals(Long256Impl.NULL_LONG256)) {
                while (!lock.compareAndSet(false, true)) {
                    Os.pause();
                }
                try {
                    sum.add(value);
                    this.count.increment();
                } finally {
                    lock.set(false);
                }
            }
        }
    }

    @Override
    public void aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId) {
        if (valueAddress == 0) {
            distinctFunc.run(pRosti, keyAddress, valueAddressSize / Long.BYTES);
        } else {
            keyValueFunc.run(pRosti, keyAddress, valueAddress, valueAddressSize / Long.BYTES, valueOffset);
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset + 1), 0);
    }

    @Override
    public void merge(long pRostiA, long pRostiB) {
        Rosti.keyedIntSumLongMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
        types.add(ColumnType.LONG);
    }

    @Override
    public void wrapUp(long pRosti) {
        Rosti.keyedIntSumLongWrapUp(pRosti, valueOffset, sum.getLong0(), count.sum());
    }

    @Override
    public void clear() {
        sum.setAll(0,0,0,0);
        count.reset();
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        Long256Impl v = (Long256Impl) getLong256A(rec);
        v.toSink(sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        if (count.sum() > 0) {
            return sum;
        }
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return getLong256A(rec);
    }
}
