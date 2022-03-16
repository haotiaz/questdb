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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class SubLong256FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "-(HH)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new SubLong256Func(args.getQuick(0), args.getQuick(1));
    }

    private static class SubLong256Func extends Long256Function implements BinaryFunction {
        final Function left;
        final Function right;

        public SubLong256Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            Long256Impl v = (Long256Impl) getLong256A(rec);
            v.toSink(sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            final Long256 x = left.getLong256A(rec);
            final Long256 y = right.getLong256A(rec);

            if (x.equals(Long256Impl.NULL_LONG256) || y.equals(Long256Impl.NULL_LONG256)) {
                return Long256Impl.NULL_LONG256;
            }

            // The difference will underflow if the top bit of x is not set and the top
            // bit of y is set (^x & y) or if they are the same (^(x ^ y)) and a borrow
            // from the lower place happens. If that borrow happens, the result will be
            // 1 - 1 - 1 = 0 - 0 - 1 = 1 (& diff).
            //borrow = ((~x & y) | (~(x ^ y) & diff)) >> 63
            long borrow = 0;
            final long l0 = x.getLong0() - y.getLong0() - borrow;
            borrow = ((~x.getLong0() & y.getLong0()) | (~(x.getLong0() ^ y.getLong0()) & l0)) >>> 63;

            final long l1 = x.getLong1() - y.getLong1() - borrow;
            borrow = ((~x.getLong1() & y.getLong1()) | (~(x.getLong1() ^ y.getLong1()) & l1)) >>> 63;

            final long l2 = x.getLong2() - y.getLong2() - borrow;
            borrow = ((~x.getLong2() & y.getLong2()) | (~(x.getLong2() ^ y.getLong2()) & l2)) >>> 63;

            final long l3 = x.getLong3() - y.getLong3() - borrow;
            //borrow = ((~x.getLong3() & y.getLong3()) | (~(x.getLong3() ^ y.getLong3()) & l3)) >>> 63;

            Long256Impl res = new Long256Impl();
            res.setAll(l0, l1, l2, l3);
            return res;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return getLong256A(rec);
        }
    }
}