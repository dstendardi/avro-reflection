package com.viadeo.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.MyCustomEncoding;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class DateCustomEncoding extends MyCustomEncoding<DateTime> {
    {
        schema = Schema.create(Schema.Type.STRING);
        schema.addProp("tag", "inst");
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
        out.writeString(encode((DateTime) datum));
    }

    @Override
    protected DateTime read(Object reuse, Decoder in) throws IOException {
        return fromString(in.readString());
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public String encode(DateTime datum) {
        return datum.toDateTime(DateTimeZone.forID("Europe/Paris")).toString();
    }

    public static DateTime fromString(String in) {
        return new DateTime(in);
    }

}