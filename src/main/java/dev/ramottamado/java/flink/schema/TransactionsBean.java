package dev.ramottamado.java.flink.schema;

import java.io.Serializable;
import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import dev.ramottamado.java.flink.util.jackson.deserializer.MicroTimestampDeserializer;
import dev.ramottamado.java.flink.util.jackson.serializer.TimestampSerializer;

/**
 * Transactions POJO.
 */
public class TransactionsBean implements Serializable {
    @JsonIgnore
    private static final long serialVersionUID = -2001L;

    @JsonProperty("src_acct")
    private String srcAccount;

    @JsonProperty("dest_acct")
    private String destAcct;

    @JsonProperty("trx_type")
    private String trxType;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("trx_timestamp")
    @JsonSerialize(using = TimestampSerializer.class)
    @JsonDeserialize(using = MicroTimestampDeserializer.class)
    private Instant trxTimestamp;

    /**
     * Transactions POJO.
     */
    public TransactionsBean() {
    }

    @JsonProperty("src_acct")
    public String getSrcAccount() {
        return srcAccount;
    }

    @JsonProperty("src_acct")
    public void setSrcAccount(String srcAccount) {
        this.srcAccount = srcAccount;
    }

    @JsonProperty("dest_acct")
    public String getDestAcct() {
        return destAcct;
    }

    @JsonProperty("dest_acct")
    public void setDestAcct(String destAcct) {
        this.destAcct = destAcct;
    }

    @JsonProperty("trx_type")
    public String getTrxType() {
        return trxType;
    }

    @JsonProperty("trx_type")
    public void setTrxType(String trxType) {
        this.trxType = trxType;
    }

    @JsonProperty("amount")
    public Double getAmount() {
        return amount;
    }

    @JsonProperty("amount")
    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @JsonProperty("trx_timestamp")
    public Instant getTrxTimestamp() {
        return trxTimestamp;
    }

    @JsonProperty("trx_timestamp")
    public void setTrxTimestamp(Instant trxTimestamp) {
        this.trxTimestamp = trxTimestamp;
    }
}
