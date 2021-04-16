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

package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.java.functions.KeySelector;

import dev.ramottamado.java.flink.schema.EnrichedTransaction;

/**
 * The {@link DestinationAccountKeySelector} implements Flink's own
 * {@link org.apache.flink.api.java.functions.KeySelector} to select {@code destAcct} as the key from
 * {@link EnrichedTransaction}.
 *
 * @author     Tamado Sitohang
 * @deprecated Replaced by {@link EnrichedTransaction#getDestAcctAsKey()}
 * @see        EnrichedTransaction#getDestAcctAsKey()
 * @since      1.0
 */
@Deprecated
public class DestinationAccountKeySelector implements KeySelector<EnrichedTransaction, String> {
    private static final long serialVersionUID = 12149238113L;

    @Override
    public String getKey(EnrichedTransaction value) {
        String destAcct = value.getDestAcct();

        return (destAcct != null) ? destAcct : "NULL";
    }
}
