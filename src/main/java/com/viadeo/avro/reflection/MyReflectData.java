package com.viadeo.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MyReflectData extends ReflectData.AllowNull {

    private static final MyReflectData INSTANCE = new MyReflectData();

    private Map<Class, MyCustomEncoding> encoders;

    public static MyReflectData get() {
        return INSTANCE;
    }

    public MyReflectData() {
        super();
        this.encoders = new HashMap<Class, MyCustomEncoding>();
    }

    public MyReflectData(Map<Class, MyCustomEncoding> encoders) {
        this();
        this.encoders.putAll(checkNotNull(encoders));
    }

    public MyReflectData addEncorder(Class clazz, MyCustomEncoding customEncoding) {
        this.encoders.put(checkNotNull(clazz), checkNotNull(customEncoding));

        return this;
    }

    /**
     * Create a schema for a field.
     */
    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {

        MyCustomEncoding customEncoding = encoders.get(field.getType());

        if (null != customEncoding) {
            return makeNullable(customEncoding.getSchema());
        }

        return super.createFieldSchema(field, names);
    }

}
