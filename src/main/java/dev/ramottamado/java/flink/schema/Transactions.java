package dev.ramottamado.java.flink.schema;

import java.time.LocalDateTime;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import dev.ramottamado.java.flink.util.jackson.deserializer.MicroTimestampDeserializer;
import dev.ramottamado.java.flink.util.jackson.serializer.TimestampSerializer;

public class Transactions {
    @JsonProperty("src_account")
    private String srcAccount;

    @JsonProperty("dest_acct")
    private Object destAcct;

    @JsonProperty("trx_type")
    private String trxType;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("trx_timestamp")
    @JsonSerialize(using = TimestampSerializer.class)
    @JsonDeserialize(using = MicroTimestampDeserializer.class)
    private LocalDateTime trxTimestamp;

    public String getSrcAccount() {
        return srcAccount;
    }

    public void setSrcAccount(String srcAccount) {
        this.srcAccount = srcAccount;
    }

    public Object getDestAcct() {
        return destAcct;
    }

    public void setDestAcct(Object destAcct) {
        this.destAcct = destAcct;
    }

    public String getTrxType() {
        return trxType;
    }

    public void setTrxType(String trxType) {
        this.trxType = trxType;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public LocalDateTime getTrxTimestamp() {
        return trxTimestamp;
    }

    public void setTrxTimestamp(LocalDateTime trxTimestamp) {
        this.trxTimestamp = trxTimestamp;
    }

}
