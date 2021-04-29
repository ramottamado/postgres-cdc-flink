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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import dev.ramottamado.java.flink.annotation.Public;

/**
 * Customer POJO.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@Public
public class Customer implements Serializable {
    @JsonIgnore
    private static final long serialVersionUID = -1999L;

    @JsonProperty("cif")
    private String cif;

    @JsonProperty("acct_number")
    private String acctNumber;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("last_name")
    private String lastName;

    @JsonProperty("city")
    private String city;

    /**
     * Customer POJO.
     *
     * @author Tamado Sitohang
     * @since  1.0
     */
    public Customer() {
    }

    @JsonProperty("cif")
    public String getCif() {
        return cif;
    }

    @JsonProperty("cif")
    public void setCif(String cif) {
        this.cif = cif;
    }

    @JsonProperty("acct_number")
    public String getAcctNumber() {
        return acctNumber;
    }

    @JsonProperty("acct_number")
    public void setAcctNumber(String acctNumber) {
        this.acctNumber = acctNumber;
    }

    @JsonProperty("first_name")
    public String getFirstName() {
        if (firstName != null)
            return firstName;
        else
            return "";
    }

    @JsonProperty("first_name")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JsonProperty("last_name")
    public String getLastName() {
        if (lastName != null)
            return lastName;
        else
            return "";
    }

    @JsonProperty("last_name")
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @JsonProperty("city")
    public String getCity() {
        return city;
    }

    @JsonProperty("city")
    public void setCity(String city) {
        this.city = city;
    }

    @JsonIgnore
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            Customer that = (Customer) o;

            return ((this.acctNumber == null ? that.getAcctNumber() == null
                    : this.acctNumber.equals(that.getAcctNumber()))
                    && (this.cif == null ? that.getCif() == null : this.cif.equals(that.getCif()))
                    && (this.city == null ? that.getCity() == null : this.city.equals(that.getCity()))
                    && (this.firstName == null ? that.getFirstName() == null
                            : this.firstName.equals(that.getFirstName()))
                    && (this.lastName == null ? that.getLastName() == null : this.lastName.equals(that.getLastName())));
        }

        return false;
    }
}
