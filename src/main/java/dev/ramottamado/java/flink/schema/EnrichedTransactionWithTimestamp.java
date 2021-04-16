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

import dev.ramottamado.java.flink.annotation.PublicEvolving;
import dev.ramottamado.java.flink.api.schema.ClassWithTimestamp;

/**
 * Enriched transaction POJO with timestamp.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@PublicEvolving
public class EnrichedTransactionWithTimestamp extends ClassWithTimestamp {
    private static final long serialVersionUID = -2005L;
    private long timestamp;
    private EnrichedTransaction etx;

    /**
     * Enriched transaction POJO with timestamp.
     *
     * @author Tamado Sitohang
     * @since  1.0
     */
    @PublicEvolving
    public EnrichedTransactionWithTimestamp() {
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public EnrichedTransaction getEtx() {
        return etx;
    }

    public void setEtx(EnrichedTransaction trx) {
        this.etx = trx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            EnrichedTransactionWithTimestamp that = (EnrichedTransactionWithTimestamp) o;

            return ((this.etx == null ? that.getEtx() == null : this.etx.equals(that.getEtx()))
                    && this.timestamp == that.getTimestamp());
        }

        return false;
    }
}
