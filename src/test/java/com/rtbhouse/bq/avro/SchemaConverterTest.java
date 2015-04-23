package com.rtbhouse.bq.avro;

import java.io.File;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.Test;

public class SchemaConverterTest {

    private static final String typeBqsc = "[{\"name\" : \"id\", \"type\" : \"INTEGER\"},{\"name\" : \"username\", \"type\" : \"STRING\"},"
        + "{\"name\" : \"passwordHash\", \"type\" : \"STRING\"},{\"name\" : \"signupDate\", \"type\" : \"INTEGER\"},"
        + "{\"name\" : \"emailAddresses\", \"type\" : \"RECORD\", \"mode\" : \"REPEATED\", \"fields\" : [{\"name\" : \"address\", \"type\" : \"STRING\"},"
        + "{\"name\" : \"verified\", \"type\" : \"BOOLEAN\"},{\"name\" : \"dateAdded\", \"type\" : \"INTEGER\"},"
        + "{\"name\" : \"dateBounced\", \"type\" : \"INTEGER\"}]},{\"name\" : \"twitterAccounts\", \"type\" : \"RECORD\", \"mode\" : \"REPEATED\", \"fields\" : "
        + "[{\"name\" : \"status\", \"type\" : \"STRING\"},{\"name\" : \"userId\", \"type\" : \"INTEGER\"},{\"name\" : \"screenName\", \"type\" : \"STRING\"},"
        + "{\"name\" : \"oauthToken\", \"type\" : \"STRING\"},{\"name\" : \"oauthTokenSecret\", \"type\" : \"STRING\"},"
        + "{\"name\" : \"dateAuthorized\", \"type\" : \"INTEGER\"}]},{\"name\" : \"toDoItems\", \"type\" : \"RECORD\", \"mode\" : \"REPEATED\", \"fields\" : "
        + "[{\"name\" : \"status\", \"type\" : \"STRING\"},{\"name\" : \"title\", \"type\" : \"STRING\"},{\"name\" : \"description\", \"type\" : \"STRING\"},"
        + "{\"name\" : \"snoozeDate\", \"type\" : \"INTEGER\"}]}]";

    @Test
    public void convertionTest() throws IOException {
        Schema avro = new Schema.Parser().parse(
            new File(getClass().getResource("/avroschema.avsc").getFile()));
        String convert = new SchemaConverter().convert(avro);
        Assert.assertNotNull(convert);
        Assert.assertEquals(typeBqsc, convert);
    }
}
