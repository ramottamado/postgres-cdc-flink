package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.java.functions.KeySelector;

import dev.ramottamado.java.flink.schema.EnrichedTransactions;

public class DestinationAccountKeySelector implements KeySelector<EnrichedTransactions, String> {

    private static final long serialVersionUID = 12149238113L;

    @Override
    public String getKey(EnrichedTransactions etx) {
        String destAcct = etx.getDestAcct();
        if (destAcct != null) {
            return etx.getDestAcct();
        } else {
            return "NULL";
        }
    }
}
