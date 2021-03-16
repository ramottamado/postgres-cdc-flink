package dev.ramottamado.java.flink.schema;

import java.time.LocalDateTime;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import dev.ramottamado.java.flink.util.jackson.deserializer.MicroTimestampDeserializer;
import dev.ramottamado.java.flink.util.jackson.serializer.TimestampSerializer;

public class EnrichedTransactions {

    public EnrichedTransactions() {
    }

    @JsonProperty("src_account")
    private String srcAccount;

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
    private LocalDateTime trxTimestamp;

    @JsonProperty("src_account")
    public String getSrcAccount() {
        return srcAccount;
    }

    @JsonProperty("src_account")
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
    public LocalDateTime getTrxTimestamp() {
        return trxTimestamp;
    }

    @JsonProperty("trx_timestamp")
    public void setTrxTimestamp(LocalDateTime trxTimestamp) {
        this.trxTimestamp = trxTimestamp;
    }
}
