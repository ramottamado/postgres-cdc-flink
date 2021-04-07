package dev.ramottamado.java.flink.schema;

import java.io.Serializable;

public class EnrichedTransactionsWithTimestampBean implements Serializable {
    private static final long serialVersionUID = -2005L;
    private long timestamp;
    private EnrichedTransactionsBean etx;

    public EnrichedTransactionsWithTimestampBean() {
    }

    public EnrichedTransactionsWithTimestampBean(long timestamp, EnrichedTransactionsBean etx) {
        this.timestamp = timestamp;
        this.etx = etx;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public EnrichedTransactionsBean getEtx() {
        return etx;
    }

    public void setEtx(EnrichedTransactionsBean trx) {
        this.etx = trx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            EnrichedTransactionsWithTimestampBean that = (EnrichedTransactionsWithTimestampBean) o;

            return ((this.etx == null ? that.getEtx() == null : this.etx.equals(that.getEtx()))
                    && this.timestamp == that.getTimestamp());
        }

        return false;
    }
}
