package org.apache.avro.reflect;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.IOException;
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

	@Override
	public DatumReader createDatumReader(Schema schema) {
		return createDatumReader(schema, schema);
	}

	@Override
	public DatumReader createDatumReader(Schema writer, Schema reader) {
		return new MyCustomDatumReader(writer, reader, this);
	}


	@Override
	public Object newRecord(Object old, Schema schema) {

		Class clazz = getClass(schema);


		Objenesis objenesis = new ObjenesisStd();
		ObjectInstantiator instantiatorOf = objenesis.getInstantiatorOf(clazz);

		return instantiatorOf.newInstance();
	}

	public static class MyCustomDatumReader extends ReflectDatumReader {
		private final MyReflectData data;


		public MyCustomDatumReader(Schema writer, Schema reader, MyReflectData data) {
			super(writer, reader, data);
			this.data = data;
		}

		@Override
		protected void readField(Object record, Schema.Field f, Object oldDatum, ResolvingDecoder in, Object state) throws IOException {
			try {
				if (f.schema().getType() == Schema.Type.UNION) {
					Schema schema = f.schema().getTypes().get(1);
					MyCustomEncoding customEncoding = data.tags.get(schema.getProp("tag"));
					if (null != customEncoding) {
						if (state != null) {
							FieldAccessor accessor = ((FieldAccessor[]) state)[f.pos()];
							if (accessor != null) {
								accessor.set(record, customEncoding.read(null, in));
							}
						}
						return;
					}
				}
			} catch (Exception e) {
				throw new AvroRuntimeException("Failed to read custom", e);
			}

			super.readField(record, f, oldDatum, in, state);
		}
	}


	@Override
	protected Schema createSchema(Type type, Map<String, Schema> names) {

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

	private void inspectClass(Type type) {
		if (type instanceof Class) {
			Class classType = (Class) type;

			/*for (Type i : classType.getGenericInterfaces()) {
				inspectClass(i);
			}*/
			inspectClass(classType.getGenericSuperclass());
		}

		if (type instanceof ParameterizedType) {
			ParameterizedType ptype = (ParameterizedType) type;
			Type[] actualTypeArguments = ptype.getActualTypeArguments();
			TypeVariable[] tps = ((Class) ptype.getRawType()).getTypeParameters();

			for (int i = 0; i < tps.length; i++) {
				addReplacement(actualTypeArguments[i], tps[i]);
			}
		}




	}

	private void addReplacement(Type actualTypeArgument, TypeVariable tp) {
		if(actualTypeArgument instanceof Class) {

			String canonicalName = ((Class)actualTypeArgument).getCanonicalName();

			if(canonicalName.startsWith("org.apache.avro")
					|| canonicalName.startsWith("org.codehaus.jackson")) {
				return;
			}
		}
		mCacheOfGenericReflection.put(tp, actualTypeArgument);
	}
}
