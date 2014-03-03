package com.viadeo.avro.reflection;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;
import org.joda.time.DateTime;

import java.io.IOException;


public class NullableDateTimeEncoding extends CustomEncoding<DateTime> {

    private DateCustomEncoding dce = new DateCustomEncoding();

    {
        schema = dce.getSchema();
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
        if (null == datum) {
            out.writeIndex(0);
            out.writeNull();
        } else {
            out.writeIndex(1);
            dce.write(datum, out);
        }
    }

    @Override
    protected DateTime read(Object reuse, Decoder in) throws IOException {

        int i = in.readIndex();
        if (0 == i) {
            in.readNull();
            return null;
        } else {
            return DateCustomEncoding.fromString(in.readString());
        }
    }
}
