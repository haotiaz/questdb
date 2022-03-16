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

package io.questdb.std;

import io.questdb.std.str.CharSink;

public class Long256Impl implements Long256, Sinkable, Long256Acceptor {

    public static final Long256Impl NULL_LONG256 = new Long256Impl();
    private long l0;
    private long l1;
    private long l2;
    private long l3;

    public static void putNull(long appendPointer) {
        Unsafe.getUnsafe().putLong(appendPointer, NULL_LONG256.getLong0());
        Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES, NULL_LONG256.getLong1());
        Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES * 2, NULL_LONG256.getLong2());
        Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES * 3, NULL_LONG256.getLong3());
    }

    public void copyFrom(Long256 value) {
        this.l0 = value.getLong0();
        this.l1 = value.getLong1();
        this.l2 = value.getLong2();
        this.l3 = value.getLong3();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
        final Long256Impl that = (Long256Impl) obj;
        return l0 == that.l0 && l1 == that.l1 && l2 == that.l2 && l3 == that.l3;
    }

    public void fromRnd(Rnd rnd) {
        setAll(
                rnd.nextLong(),
                rnd.nextLong(),
                rnd.nextLong(),
                rnd.nextLong()
        );
    }

    @Override
    public long getLong0() {
        return l0;
    }

    @Override
    public long getLong1() {
        return l1;
    }

    @Override
    public long getLong2() {
        return l2;
    }

    @Override
    public long getLong3() {
        return l3;
    }

    @Override
    public void setAll(long l0, long l1, long l2, long l3) {
        this.l0 = l0;
        this.l1 = l1;
        this.l2 = l2;
        this.l3 = l3;
    }

    @Override
    public void toSink(CharSink sink) {
        Numbers.appendLong256(l0, l1, l2, l3, sink);
    }

    public void add(long v0, long v1, long v2, long v3) {
        boolean isNull = v0 == Numbers.LONG_NaN &&
                v1 == Numbers.LONG_NaN &&
                v2 == Numbers.LONG_NaN &&
                v3 == Numbers.LONG_NaN;

        if (isNull) {
            this.setAll(Numbers.LONG_NaN,
                    Numbers.LONG_NaN,
                    Numbers.LONG_NaN,
                    Numbers.LONG_NaN);
        } else {
            // The sum will overflow if both top bits are set (x & y) or if one of them
            // is (x | y), and a carry from the lower place happened. If such a carry
            // happens, the top bit will be 1 + 0 + 1 = 0 (& ~sum).
            long carry = 0;
            final long l0 = v0 + this.getLong0() + carry;
            carry = ((v0 & this.getLong0()) | ((v0 | this.getLong0()) & ~l0)) >>> 63;

            final long l1 = v1 + this.getLong1() + carry;
            carry = ((v1 & this.getLong1()) | ((v1 | this.getLong1()) & ~l1)) >>> 63;

            final long l2 = v2 + this.getLong2() + carry;
            carry = ((v2 & this.getLong2()) | ((v2 | this.getLong2()) & ~l2)) >>> 63;

            final long l3 = v3 + this.getLong3() + carry;
            //carry = ((v3 & this.getLong3()) | ((v3 | this.getLong3()) & ~l3)) >>> 63;

            this.setAll(l0, l1, l2, l3);
        }
    }

    public void add(final Long256 x) {
        this.add(x.getLong0(), x.getLong1(), x.getLong2(), x.getLong3());
    }

    public static Long256Impl add(final Long256 x, final Long256 y) {
        Long256Impl sum = new Long256Impl();
        sum.copyFrom(x);
        sum.add(y);
        return sum;
    }

    static {
        NULL_LONG256.setAll(
                Numbers.LONG_NaN,
                Numbers.LONG_NaN,
                Numbers.LONG_NaN,
                Numbers.LONG_NaN
        );
    }
}
