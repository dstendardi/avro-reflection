package org.apache.avro.reflect;


import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import java.io.IOException;

public abstract class MyCustomEncoding<T> extends CustomEncoding<T> {

    public Schema getSchema() {
        return schema;
    }

    public abstract Object encode(T datum);

    protected abstract void write(Object datum, Encoder out) throws IOException;

    protected abstract T read(Object reuse, Decoder in) throws IOException;
}
