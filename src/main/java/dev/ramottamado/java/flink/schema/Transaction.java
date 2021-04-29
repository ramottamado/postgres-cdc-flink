/*
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink.schema;

import java.io.Serializable;
import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import dev.ramottamado.java.flink.annotation.Public;
import dev.ramottamado.java.flink.util.jackson.deserializer.MicroTimestampDeserializer;
import dev.ramottamado.java.flink.util.jackson.serializer.TimestampSerializer;

/**
 * Transaction POJO.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@Public
public class Transaction implements Serializable {
    @JsonIgnore
    private static final long serialVersionUID = -2001L;

    @JsonProperty("src_acct")
    private String srcAcct;

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
     * Transaction POJO.
     *
     * @author Tamado Sitohang
     * @since  1.0
     */
    public Transaction() {
    }

    @JsonProperty("src_acct")
    public String getSrcAcct() {
        return srcAcct;
    }

    @JsonProperty("src_acct")
    public void setSrcAcct(String srcAcct) {
        this.srcAcct = srcAcct;
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

    @JsonIgnore
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            Transaction that = (Transaction) o;

            return ((this.amount == null ? that.getAmount() == null : this.amount.equals(that.getAmount()))
                    && (this.destAcct == null ? that.getDestAcct() == null : this.destAcct.equals(that.getDestAcct()))
                    && (this.srcAcct == null ? that.getSrcAcct() == null : this.srcAcct.equals(that.getSrcAcct()))
                    && (this.trxTimestamp == null ? that.getTrxTimestamp() == null
                            : this.trxTimestamp.equals(that.getTrxTimestamp()))
                    && (this.trxType == null ? that.getTrxType() == null
                            : this.trxType.equals(that.getTrxType())));
        }

        return false;
    }
}
