package com.viadeo.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;

import java.io.IOException;

/**
 * Created by dstendardi on 03/03/14.
 */
abstract class MyCustomEncoding<T> extends CustomEncoding {

    public Schema getSchema() {
        return schema;
    }

    protected abstract void write(Object datum, Encoder out) throws IOException;

    protected abstract T read(Object reuse, Decoder in) throws IOException;
}
