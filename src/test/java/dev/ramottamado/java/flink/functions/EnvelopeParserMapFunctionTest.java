package dev.ramottamado.java.flink.functions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import dev.ramottamado.java.flink.schema.CustomersBean;

public class EnvelopeParserMapFunctionTest {
    private String dummyEnvelope;
    private CustomersBean expected;

    private CustomersBean createExpectedCustomers() {
        CustomersBean expected = new CustomersBean();
        expected.setCif("029817127819");
        expected.setAcctNumber("067637881");
        expected.setCity("Jakarta");
        expected.setFirstName("Taufiq");
        expected.setLastName("Maulana");

        return expected;
    }

    @Test
    public void testMap() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        expected = createExpectedCustomers();

        dummyEnvelope = String.join("\n",
                "{",
                "   \"schema\" : {},",
                "   \"payload\" : {",
                "       \"op\": \"i\",",
                "       \"source\": {},",
                "       \"ts_ms\" : \"\",",
                "       \"before\" : {",
                "       },",
                "       \"after\" : {",
                "          \"cif\" : \"029817127819\",",
                "          \"acct_number\" : \"067637881\",",
                "          \"first_name\" : \"Taufiq\",",
                "          \"last_name\" : \"Maulana\",",
                "          \"city\" : \"Jakarta\"",
                "       }",
                "   }",
                "}");

        ObjectNode dummyObjectNode = mapper.createObjectNode();
        dummyObjectNode.set("value", mapper.readTree(dummyEnvelope));

        EnvelopeParserMapFunction<CustomersBean> customersEnvelopeParserMapFunction =
                new EnvelopeParserMapFunction<>(CustomersBean.class);
        CustomersBean out = customersEnvelopeParserMapFunction.map(dummyObjectNode);

        Assert.assertNotNull(out);
        Assert.assertEquals("Result not as expected.", expected.getAcctNumber(), out.getAcctNumber());
        Assert.assertEquals("Result not as expected.", expected.getCif(), out.getCif());
        Assert.assertEquals("Result not as expected.", expected.getCity(), out.getCity());
        Assert.assertEquals("Result not as expected.", expected.getFirstName(), out.getFirstName());
        Assert.assertEquals("Result not as expected.", expected.getLastName(), out.getLastName());
    }
}
