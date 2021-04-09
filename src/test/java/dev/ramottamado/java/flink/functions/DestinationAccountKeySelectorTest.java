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

import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.EnrichedTransactions;

@SuppressWarnings("deprecation")
public class DestinationAccountKeySelectorTest {
    private EnrichedTransactions etx;
    private DestinationAccountKeySelector selector;

    @Before
    public void prepareTest() {
        selector = new DestinationAccountKeySelector();

        etx = new EnrichedTransactions();
        etx.setAmount(10000.0);
        etx.setDestAcct("0002");
        etx.setSrcAcct("0001");
        etx.setTrxTimestamp(Instant.parse("2021-01-01T12:00:00.00Z"));
        etx.setTrxType("TRANSFER");
        etx.setCif("001");
        etx.setSrcName("Tamado Sitohang");
    }

    @Test
    public void testGetKey() {
        String key = selector.getKey(etx);

        Assert.assertNotNull(key);
        Assert.assertEquals("0002", key);
    }

    @Test
    public void testGetNullKey() {
        etx.setDestAcct(null);
        String actual = selector.getKey(etx);

        Assert.assertNotNull(actual);
        Assert.assertEquals("NULL", actual);
    }
}
