package dev.ramottamado.java.flink;

import static dev.ramottamado.java.flink.config.ParameterConfig.CHECKPOINT_PATH;
import static dev.ramottamado.java.flink.config.ParameterConfig.RUNNING_ENVIRONMENT;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import dev.ramottamado.java.flink.functions.EnrichEnrichedTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.functions.EnrichTransactionsWithCustomersJoinFunction;
import dev.ramottamado.java.flink.schema.Customers;
import dev.ramottamado.java.flink.schema.EnrichedTransactions;
import dev.ramottamado.java.flink.schema.Transactions;


public abstract class TransactionsEnrichmentStreamingJob {

    protected abstract DataStream<Transactions> readTransactionsCdcStream(
            StreamExecutionEnvironment env, ParameterTool params
    ) throws Exception;

    protected abstract DataStream<Customers> readCustomersCdcStream(
            StreamExecutionEnvironment env, ParameterTool params
    ) throws Exception;

    protected abstract void writeEnrichedTransactionsOutput(
            DataStream<EnrichedTransactions> enrichedTrxStream, ParameterTool params
    ) throws Exception;

    public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) throws Exception {

        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        KeyedStream<Customers, String> keyedCustomersCdcStream = readCustomersCdcStream(env, params)
                .keyBy(Customers::getAcctNumber);

        KeyedStream<Transactions, String> keyedTransactionsStream = readTransactionsCdcStream(env, params)
                .keyBy(Transactions::getSrcAccount);

        KeyedStream<EnrichedTransactions, String> enrichedTrxStream = keyedTransactionsStream
                .connect(keyedCustomersCdcStream)
                .process(new EnrichTransactionsWithCustomersJoinFunction())
                .uid("enriched_transactions")
                .keyBy(EnrichedTransactions::getDestAcct);

        SingleOutputStreamOperator<EnrichedTransactions> enrichedTrxStream2 = enrichedTrxStream
                .connect(keyedCustomersCdcStream)
                .process(new EnrichEnrichedTransactionsWithCustomersJoinFunction())
                .uid("enriched_transactions_2");

        writeEnrichedTransactionsOutput(enrichedTrxStream2, params);

        return env;
    }

    private StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) throws Exception {

        StreamExecutionEnvironment env;
        String checkpointPath = params.getRequired(CHECKPOINT_PATH);
        StateBackend stateBackend = new FsStateBackend(checkpointPath);
        Configuration conf = new Configuration();

        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.dir", checkpointPath);

        if (params.get(RUNNING_ENVIRONMENT) == "development") {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStateBackend(stateBackend);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.enableCheckpointing(10000L);

        return env;
    }
}
