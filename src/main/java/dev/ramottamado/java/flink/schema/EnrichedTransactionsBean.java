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
 * Enriched transactions POJO.
 */
public class EnrichedTransactionsBean implements Serializable {
    @JsonIgnore
    private static final long serialVersionUID = -2000L;

    @JsonProperty("src_acct")
    private String srcAcct;

    @JsonProperty("dest_acct")
    private String destAcct;

    @JsonProperty("trx_type")
    private String trxType;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("cif")
    private String cif;

    @JsonProperty("src_name")
    private String srcName;

    @JsonProperty("dest_name")
    private String destname;

    @JsonProperty("trx_timestamp")
    @JsonSerialize(using = TimestampSerializer.class)
    @JsonDeserialize(using = MicroTimestampDeserializer.class)
    private Instant trxTimestamp;

    /**
     * Enriched transactions POJO.
     */
    public EnrichedTransactionsBean() {
    }

    @JsonProperty("src_acct")
    public String getSrcAccount() {
        return srcAcct;
    }

    @JsonProperty("src_acct")
    public void setSrcAccount(String srcAccount) {
        this.srcAcct = srcAccount;
    }

    @JsonProperty("dest_acct")
    public String getDestAcct() {
        return (destAcct != null) ? destAcct : "NULL";
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

    @JsonProperty("cif")
    public String getCif() {
        return cif;
    }

    @JsonProperty("cif")
    public void setCif(String cif) {
        this.cif = cif;
    }

    @JsonProperty("src_name")
    public String getSrcName() {
        return srcName;
    }

    @JsonProperty("src_name")
    public void setSrcName(String srcName) {
        this.srcName = srcName;
    }

    @JsonProperty("dest_name")
    public String getDestName() {
        return destname;
    }

    @JsonProperty("dest_name")
    public void setDestName(String destName) {
        this.destname = destName;
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
