package com.viadeo.avro.reflection;

import org.apache.avro.reflect.CustomEncoding;
import org.apache.avro.reflect.ReflectData;

import java.util.Map;

public class MyReflectData extends ReflectData {

    private Map<Class, CustomEncoding> encoders;

    public MyReflectData() {
        super();
    }

    public MyReflectData(Map<Class, CustomEncoding> encoders) {
        this();
        this.encoders = encoders;
    }

    public MyReflectData addEncorder(Class clazz, CustomEncoding customEncoding) {
        this.encoders.put(clazz, customEncoding);

        return this;
    }
}
