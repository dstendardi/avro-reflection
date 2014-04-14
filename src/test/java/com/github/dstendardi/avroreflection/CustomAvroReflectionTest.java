package com.github.dstendardi.avroreflection;

import com.viadeo.avro.reflection.DateCustomEncoding;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.MyReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CustomAvroReflectionTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    MyReflectData MY_REFLECT_DATA = new MyReflectData().addEncorder(DateTime.class, "inst", new DateCustomEncoding());

	private ClassExtendingAGenericClass event = new ClassExtendingAGenericClass(new DateTime("2012-10-12"), "optionalField");

	public static class GenericParentClass<T> {

		private final T encodedField;

		public GenericParentClass(T encodedField) {
			this.encodedField = encodedField;
		}

		public T getEncodedField() {
			return encodedField;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			GenericParentClass that = (GenericParentClass) o;

			if (encodedField != null ? !encodedField.equals(that.encodedField) : that.encodedField != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return encodedField != null ? encodedField.hashCode() : 0;
		}
	}

	public static class ClassExtendingAGenericClass extends GenericParentClass<DateTime> {

		private String optionalField;


		public ClassExtendingAGenericClass(DateTime encodedField) {
			super(encodedField);
		}


		public ClassExtendingAGenericClass(DateTime encodedField, String optionalField) {
			super(encodedField);
			this.optionalField = optionalField;
		}


		public String getOptionalField() {
			return optionalField;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ClassExtendingAGenericClass that = (ClassExtendingAGenericClass) o;

			if (optionalField != null ? !optionalField.equals(that.optionalField) : that.optionalField != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return optionalField != null ? optionalField.hashCode() : 0;
		}


	}

	public static class WrappedGenericField<T> {
		private final T wrappedGenericField;

		public WrappedGenericField(T wrappedGenericField) {
			this.wrappedGenericField = wrappedGenericField;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			WrappedGenericField that = (WrappedGenericField) o;

			if (wrappedGenericField != null ? !wrappedGenericField.equals(that.wrappedGenericField) : that.wrappedGenericField != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return wrappedGenericField != null ? wrappedGenericField.hashCode() : 0;
		}
	}

	public static class ClassWithOneUnmaterializedField {
		private final WrappedGenericField<String> one;

		public ClassWithOneUnmaterializedField(WrappedGenericField<String> wrappedGenericField) {
			this.one = wrappedGenericField;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ClassWithOneUnmaterializedField that = (ClassWithOneUnmaterializedField) o;

			if (one != null ? !one.equals(that.one) : that.one != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return one != null ? one.hashCode() : 0;
		}
	}

    public static class ClassWithTwoUnmaterializedField {
        private final WrappedGenericField<String> one;
        private final WrappedGenericField<Long> two;

        public ClassWithTwoUnmaterializedField(WrappedGenericField<String> wrappedGenericField, WrappedGenericField<Long> two) {
            this.one = wrappedGenericField;
            this.two = two;
        }
    }


    public static class BloatedWrapper {
        private final ClassWithTwoUnmaterializedField value;


        public BloatedWrapper(ClassWithTwoUnmaterializedField value) {
            this.value = value;
        }
    }



	Schema SCHEMA_WITH_ONE_PARAMETRIZED_TYPE = MY_REFLECT_DATA.getSchema(ClassWithOneUnmaterializedField.class);
	Schema SCHEMA = MY_REFLECT_DATA.getSchema(ClassExtendingAGenericClass.class);



	@Test
	public void schema_with_custom_encoder() throws Exception {

		assertNotNull(SCHEMA.getField("encodedField"));
		Schema dateTimeSchema = SCHEMA.getField("encodedField").schema().getTypes().get(1);
		Schema.Type dateTime = dateTimeSchema.getType();
		assertEquals(dateTime, Schema.Type.STRING);
		assertEquals("inst", dateTimeSchema.getProp("tag"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void serialization_with_custom_encoder() throws Exception {
		ReflectDatumWriter<ClassExtendingAGenericClass> writer = (ReflectDatumWriter<ClassExtendingAGenericClass>) MY_REFLECT_DATA.createDatumWriter(SCHEMA);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().jsonEncoder(SCHEMA, os);
		writer.write(event, encoder);
		encoder.flush();
		os.flush();

		assertEquals("{\"optionalField\":{\"string\":\"optionalField\"},\"encodedField\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"}}", os.toString("UTF-8"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void deserialization_with_custom_encoder() throws IOException {

		ReflectDatumReader<ClassExtendingAGenericClass> reader = (ReflectDatumReader<ClassExtendingAGenericClass>) MY_REFLECT_DATA.createDatumReader(SCHEMA);
		ClassExtendingAGenericClass actual = reader.read(null, DecoderFactory.get().jsonDecoder(SCHEMA, "{\"encodedField\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"},\"optionalField\":{\"string\":\"optionalField\"}}"));

		assertEquals(event, actual);
	}


	@Test
	public void serialization_with_optional_parameter() throws Exception {

		ReflectDatumWriter<ClassExtendingAGenericClass> writer = (ReflectDatumWriter<ClassExtendingAGenericClass>) MY_REFLECT_DATA.createDatumWriter(SCHEMA);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().jsonEncoder(SCHEMA, os);
		writer.write(new ClassExtendingAGenericClass(new DateTime("2012-10-12")), encoder);
		encoder.flush();
		os.flush();

		assertEquals("{\"optionalField\":null,\"encodedField\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"}}", os.toString("UTF-8"));
	}

	@Test
	public void deserialization_with_optional_parameter() throws Exception {

		ReflectDatumReader<ClassExtendingAGenericClass> reader = (ReflectDatumReader<ClassExtendingAGenericClass>) MY_REFLECT_DATA.createDatumReader(SCHEMA);
		ClassExtendingAGenericClass actual = reader.read(null, DecoderFactory.get().jsonDecoder(SCHEMA, "{\"encodedField\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"},\"optionalField\":null}"));

		assertEquals(new ClassExtendingAGenericClass(new DateTime("2012-10-12")), actual);
	}


	@Test
	public void serialization_with_parametrized_type() throws Exception {

		ReflectDatumWriter<ClassWithOneUnmaterializedField> writer = (ReflectDatumWriter<ClassWithOneUnmaterializedField>) MY_REFLECT_DATA.createDatumWriter(SCHEMA_WITH_ONE_PARAMETRIZED_TYPE);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().jsonEncoder(SCHEMA_WITH_ONE_PARAMETRIZED_TYPE, os);
		writer.write(new ClassWithOneUnmaterializedField(new WrappedGenericField<String>("optionalField")), encoder);
		encoder.flush();
		os.flush();


		assertEquals("{\"one\":{\"com.github.dstendardi.avroreflection.CustomAvroReflectionTest$.WrappedGenericField\":{\"wrappedGenericField\":{\"string\":\"optionalField\"}}}}", os.toString("UTF-8"));
	}

	@Test
	public void deserialization_with_parametrized_type() throws Exception {

		ReflectDatumReader<ClassWithOneUnmaterializedField> reader = (ReflectDatumReader<ClassWithOneUnmaterializedField>) MY_REFLECT_DATA.createDatumReader(SCHEMA_WITH_ONE_PARAMETRIZED_TYPE);
		ClassWithOneUnmaterializedField actual = reader.read(null, DecoderFactory.get().jsonDecoder(SCHEMA_WITH_ONE_PARAMETRIZED_TYPE, "{\"one\":{\"com.github.dstendardi.avroreflection.CustomAvroReflectionTest$.WrappedGenericField\":{\"wrappedGenericField\":{\"string\":\"optionalField\"}}}}"));

		assertEquals(new ClassWithOneUnmaterializedField(new WrappedGenericField<String>("optionalField")), actual);
	}


    @Test
    public void serialization_with_two_parametrized_type() throws Exception {

        exception.expect(RuntimeException.class);
        exception.expectMessage(containsString("you cannot have several generics members for the same class 'com.github.dstendardi.avroreflection.CustomAvroReflectionTest.WrappedGenericField"));

        MY_REFLECT_DATA.getSchema(ClassWithTwoUnmaterializedField.class);
    }

    @Test
    public void serialization_with_two_parametrized_type_wrapped() throws Exception {

        exception.expect(RuntimeException.class);
        exception.expectMessage(containsString("you cannot have several generics members for the same class 'com.github.dstendardi.avroreflection.CustomAvroReflectionTest.WrappedGenericField"));

        MY_REFLECT_DATA.getSchema(BloatedWrapper.class);
    }



}

