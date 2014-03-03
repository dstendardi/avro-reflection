package com.viadeo.avro.reflection;

import org.apache.avro.reflect.CustomEncoding;
import org.apache.avro.reflect.ReflectData;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MyReflectData extends ReflectData.AllowNull {

    private static final MyReflectData INSTANCE = new MyReflectData();

    private Map<Class, CustomEncoding> encoders;

    public static MyReflectData get() { return INSTANCE; }
    
    public MyReflectData() {
        super();
        this.encoders = new HashMap<Class, CustomEncoding>();
    }

    public MyReflectData(Map<Class, CustomEncoding> encoders) {
        this();
        this.encoders.putAll(checkNotNull(encoders));
    }

    public MyReflectData addEncorder(Class clazz, CustomEncoding customEncoding) {
        this.encoders.put(checkNotNull(clazz), checkNotNull(customEncoding));

        return this;
    }


}
