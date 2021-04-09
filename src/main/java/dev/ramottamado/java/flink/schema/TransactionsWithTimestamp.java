/*
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink.schema;

import dev.ramottamado.java.flink.api.schema.ClassWithTimestamp;

/**
 * Transactions POJO with timestamp.
 */
public class TransactionsWithTimestamp extends ClassWithTimestamp {
    private static final long serialVersionUID = -2003L;
    private long timestamp;
    private Transactions trx;

    /**
     * Transactions POJO with timestamp.
     */
    public TransactionsWithTimestamp() {
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Transactions getTrx() {
        return trx;
    }

    public void setTrx(Transactions trx) {
        this.trx = trx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            TransactionsWithTimestamp that = (TransactionsWithTimestamp) o;

            return ((this.trx == null ? that.getTrx() == null : this.trx.equals(that.getTrx()))
                    && this.timestamp == that.getTimestamp());
        }

        return false;
    }
}
