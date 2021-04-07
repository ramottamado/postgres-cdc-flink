package dev.ramottamado.java.flink.schema;

import java.io.Serializable;

public class TransactionsWithTimestampBean implements Serializable {
    private static final long serialVersionUID = -2003L;
    private long timestamp;
    private TransactionsBean trx;

    public TransactionsWithTimestampBean() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            TransactionsWithTimestampBean that = (TransactionsWithTimestampBean) o;

            return ((this.trx == null ? that.getTrx() == null : this.trx.equals(that.getTrx()))
                    && this.timestamp == that.getTimestamp());
        }

        return false;
    }
}
