package com.viadeo.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class DateCustomEncoder extends CustomEncoding<DateTime> {
    {
        schema = Schema.create(Schema.Type.STRING);
        schema.addProp("tag", "inst");
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
        out.writeString(((DateTime) datum).toDateTime(DateTimeZone.forID("Europe/Paris")).toString());
    }

    @Override
    protected DateTime read(Object reuse, Decoder in) throws IOException {
        return new DateTime(in.readString());
    }

    protected Schema getSchema() {
        return schema;
    }

}