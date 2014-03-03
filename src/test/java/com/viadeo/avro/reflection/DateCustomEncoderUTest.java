package com.viadeo.avro.reflection;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

public class DateCustomEncoderUTest {

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    private JsonEncoder jsonEncoder;
    private DateCustomEncoder encoder;
    private ByteArrayInputStream input;

    @Before
    public void setUp() throws Exception {
        encoder = new DateCustomEncoder();
        jsonEncoder = EncoderFactory.get().jsonEncoder(encoder.getSchema(), out);
    }

    @Test
    public void write_with_different_timezones() throws Exception {

        DateTime dateTime = new DateTime("2012-10-12", DateTimeZone.forID("Europe/Paris"))
                .toDateTime(DateTimeZone.forID("EET"));

        encoder.write(dateTime, jsonEncoder);

        jsonEncoder.flush();
        out.flush();

        assertEquals("\"2012-10-12T00:00:00.000+02:00\"", out.toString());
    }


    @Test
    public void read() throws Exception {

        input = new ByteArrayInputStream("\"2014-01-01T23:00:00.000-00:00\"".getBytes());

        DateTime given = encoder.read(null, DecoderFactory.get().jsonDecoder(encoder.getSchema(), input));

        assertEquals(new DateTime("2014-01-02", DateTimeZone.forID("Europe/Paris")), given);
    }
}
