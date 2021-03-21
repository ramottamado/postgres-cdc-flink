package dev.ramottamado.java.flink.schema;

import java.io.Serializable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Customers POJO.
 */
public class CustomersBean implements Serializable {
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
     * Customers POJO.
     */
    public CustomersBean() {
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
        return firstName;
    }

    @JsonProperty("first_name")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JsonProperty("last_name")
    public String getLastName() {
        return lastName;
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
}