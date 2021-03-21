package dev.ramottamado.java.flink.functions;

import org.apache.flink.api.java.functions.KeySelector;

import dev.ramottamado.java.flink.schema.EnrichedTransactionsBean;

/**
 * The {@link DestinationAccountKeySelector} implements Flink's own
 * {@link org.apache.flink.api.java.functions.KeySelector} to select {@code destAcct} as the key from
 * {@link EnrichedTransactionsBean}.
 *
 * @deprecated Replaced by {@link EnrichedTransactionsBean#getDestAcct()}
 * @see        EnrichedTransactionsBean#getDestAcct()
 */
@Deprecated
public class DestinationAccountKeySelector implements KeySelector<EnrichedTransactionsBean, String> {
    private static final long serialVersionUID = 12149238113L;

    @Override
    public String getKey(EnrichedTransactionsBean value) {
        String destAcct = value.getDestAcct();

        return (destAcct != null) ? destAcct : null;
    }
}
