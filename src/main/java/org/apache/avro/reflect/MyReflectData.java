package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MyReflectData extends ReflectData.AllowNull {

    private static final MyReflectData INSTANCE = new MyReflectData();

    private Map<Class, MyCustomEncoding> encoders;
    protected Map<String, MyCustomEncoding> tags;

    private Map<TypeVariable, Type> mCacheOfGenericReflection;

    public static MyReflectData get() {
        return INSTANCE;
    }

    public MyReflectData() {
        super();
        this.encoders = new HashMap<Class, MyCustomEncoding>();
        this.tags = new HashMap<String, MyCustomEncoding>();
        this.mCacheOfGenericReflection = new HashMap<TypeVariable, Type>();
    }


    public MyReflectData addEncorder(Class clazz, String tag, MyCustomEncoding customEncoding) {
        this.encoders.put(checkNotNull(clazz), checkNotNull(customEncoding));
        this.tags.put(checkNotNull(tag), checkNotNull(customEncoding));

        return this;
    }

    //*********** WRITING ********************//
    @Override
    protected Object getField(Object record, String name, int pos, Object state) {

        Object field = super.getField(record, name, pos, state);

        if (null == field) {
            return null;
        }

        MyCustomEncoding customEncoding = encoders.get(field.getClass());

        if (null != customEncoding) {
            //noinspection unchecked
            return customEncoding.encode(field);
        } else {
            return field;
        }
    }


    //*************** READING *****************//

    /**
     * need to be overrided, to support class without empty constructor
     */
    @Override
    public Object newRecord(Object old, Schema schema) {

        Class clazz = getClass(schema);


        Objenesis objenesis = new ObjenesisStd();
        ObjectInstantiator instantiatorOf = objenesis.getInstantiatorOf(clazz);

        return instantiatorOf.newInstance();
    }


    @Override
    public DatumReader createDatumReader(Schema schema) {
        return createDatumReader(schema, schema);
    }

    @Override
    public DatumReader createDatumReader(Schema writer, Schema reader) {
        return new MyCustomDatumReader(writer, reader, this);
    }


    /**
     * need to overload `read` to support custom decoding
     */
    public static class MyCustomDatumReader extends ReflectDatumReader {
        private final MyReflectData data;


        public MyCustomDatumReader(Schema writer, Schema reader, MyReflectData data) {
            super(writer, reader, data);
            this.data = data;
        }

        @Override
        protected Object read(Object old, Schema expected, ResolvingDecoder in) throws IOException {
            MyCustomEncoding customEncoding = data.tags.get(expected.getProp("tag"));
            if (customEncoding != null) {
                return customEncoding.read(null, in);
            }

            return super.read(old, expected, in);
        }

    }


    //***************** SCHEMA CREATION **************//


    @Override
    protected Schema createSchema(Type type, Map<String, Schema> names) {

        guardSchemaCreation(type);

        inspectClass(type);


        MyCustomEncoding customEncoding = encoders.get(type);
        if (null != customEncoding) {
            return customEncoding.getSchema();
        }

        Type replacementType = mCacheOfGenericReflection.get(type);
        if (replacementType != null) {
            return createSchema(replacementType, names);
        }

        return super.createSchema(type, names);
    }

    protected void guardSchemaCreation(Type type) {
        if (type instanceof Class) {
            Map<String, String> countDumClass = new HashMap<String, String>();
            Class<?> c = (Class<?>) type;
            do {
                for (Field field : c.getDeclaredFields()) {
                    Type genericType = field.getGenericType();
                    if (genericType instanceof ParameterizedType) {
                        String name = ((Class) ((ParameterizedType) genericType).getRawType()).getCanonicalName();
                        String otherWhere = countDumClass.get(name);
                        String where = c.getCanonicalName() + "/" + field.getName();
                        if (otherWhere != null) {
                            String format = String.format("you cannot have several generics members for the same class '%s' : \n %s \n %s ", name, otherWhere, where);
                            throw new RuntimeException(format);
                        }
                        countDumClass.put(name, where);
                    }
                }
                c = c.getSuperclass();
            } while (c != null);
        }
    }




    /**
     * inspect by reflection the class hierarchy to initialize a replacement map.
     * <p/>
     * The map contains information such as :
     * {
     * blabla.GenericParentClass.T   org.joda.time.DateTime,
     * blabla.GenericParentClass2.K  java.lang.String
     * }
     *
     * @param type the type to inspect
     */
    private void inspectClass(Type type) {
        // inspect parent
        if (type instanceof Class) {
            Class classType = (Class) type;

            inspectClass(classType.getGenericSuperclass());
        }

        // if the current type is a generic
        if (type instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType) type;
            Type[] actualTypeArguments = ptype.getActualTypeArguments();
            TypeVariable[] tps = ((Class) ptype.getRawType()).getTypeParameters();

            // for each couple of [typeVariable actualType] ( [T java.lang.String] )
            for (int i = 0; i < tps.length; i++) {
                addReplacement(actualTypeArguments[i], tps[i]);
            }
        }
    }

    private void addReplacement(Type actualTypeArgument, TypeVariable tp) {
        mCacheOfGenericReflection.put(tp, actualTypeArgument);
    }
}
