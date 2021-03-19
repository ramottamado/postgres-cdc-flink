package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class DestinationAccountKeySelector implements KeySelector<EnrichedTransactions, String> {

    private static final long serialVersionUID = 12149238113L;

    private static final Logger logger = LoggerFactory.getLogger(DestinationAccountKeySelector.class);

    @Override
    public String getKey(EnrichedTransactions etx) {
        String destAcct = etx.getDestAcct();

        if (destAcct != null) {
            return destAcct;
        } else {
            return "NULL";
        }
    }
}
