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

package dev.ramottamado.java.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import dev.ramottamado.java.flink.annotation.Public;
import dev.ramottamado.java.flink.functions.EnrichEnrichedTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.schema.Customer;
import dev.ramottamado.java.flink.schema.EnrichedTransaction;
import dev.ramottamado.java.flink.schema.Transaction;

/**
 * The abstract class {@code AbstractTransactionsEnrichmentStreamingJob} provides base class, logic and pipeline for
 * enriching {@link Transaction} data using Flink. The core pipeline and functionality is encapsulated here, while
 * subclasses have to implement input and output methods. Check {@link KafkaTransactionsEnrichmentStreamingJob} for the
 * implementation using data stream from Kafka.
 *
 * @author Tamado Sitohang
 * @see    KafkaTransactionsEnrichmentStreamingJob
 * @since  1.0
 */
@Public
public abstract class AbstractTransactionsEnrichmentStreamingJob {
    public static ParameterTool params;
    public StreamExecutionEnvironment env;

    /**
     * Method to get {@link Transaction} data stream.
     *
     * @return                  the {@link Transaction} data stream
     * @throws RuntimeException if input cannot be read.
     * @since                   1.0
     */
    public abstract DataStream<Transaction> readTransactionsCdcStream() throws RuntimeException;

    /**
     * Method to get {@link Customer} data stream.
     *
     * @return                  the {@link Customer} data stream
     * @throws RuntimeException if input cannot be read.
     * @since                   1.0
     */
    public abstract DataStream<Customer> readCustomersCdcStream() throws RuntimeException;

    /**
     * Method to write {@link EnrichedTransaction} data stream to sink.
     *
     * @param  enrichedTrxStream the {@link EnrichedTransaction} data stream
     * @throws RuntimeException  if output cannot be written
     * @since                    1.0
     */
    public abstract void writeEnrichedTransactionsOutput(DataStream<EnrichedTransaction> enrichedTrxStream)
            throws RuntimeException;

    /**
     * Method to get {@link StreamExecutionEnvironment}.
     *
     * @return                  the {@link StreamExecutionEnvironment} to run the pipeline
     * @throws RuntimeException if something wrong happened
     * @since                   1.0
     */
    public abstract StreamExecutionEnvironment createExecutionEnvironment()
            throws RuntimeException;

    /**
     * The core logic and pipeline for enriching {@link Transaction} data stream using data from
     * {@link Customer}.
     *
     * @return                  the Flink {@link StreamExecutionEnvironment} environment
     * @throws RuntimeException if input/output cannot be read/write
     * @since                   1.0
     */
    public final StreamExecutionEnvironment createApplicationPipeline() throws RuntimeException {
        env = createExecutionEnvironment();

        final KeyedStream<Customer, String> keyedCustomersCdcStream = readCustomersCdcStream()
                .keyBy(Customer::getAcctNumber);

        final KeyedStream<Transaction, String> keyedTransactionsStream = readTransactionsCdcStream()
                .keyBy(Transaction::getSrcAcct);

        final KeyedStream<EnrichedTransaction, String> enrichedTrxStream = keyedTransactionsStream
                .connect(keyedCustomersCdcStream)
                .process(new EnrichTransactionsWithCustomersJoinFunction())
                .uid("enriched_transactions")
                .keyBy(EnrichedTransaction::getDestAcctAsKey);

        final SingleOutputStreamOperator<EnrichedTransaction> enrichedTrxStream2 = enrichedTrxStream
                .connect(keyedCustomersCdcStream)
                .process(new EnrichEnrichedTransactionsWithCustomersJoinFunction())
                .uid("enriched_transactions_2");

        writeEnrichedTransactionsOutput(enrichedTrxStream2);

        return env;
    }
}
