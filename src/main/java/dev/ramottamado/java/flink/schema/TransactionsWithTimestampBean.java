package dev.ramottamado.java.flink.schema;

import java.io.Serializable;

public class TransactionsWithTimestampBean implements Serializable {
    private static final long serialVersionUID = -2003L;
    private long timestamp;
    private TransactionsBean trx;

    public TransactionsWithTimestampBean() {
    }

    public TransactionsWithTimestampBean(long timestamp, TransactionsBean trx) {
        this.timestamp = timestamp;
        this.trx = trx;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TransactionsBean getTrx() {
        return trx;
    }

    public void setTrx(TransactionsBean trx) {
        this.trx = trx;
    }
}
