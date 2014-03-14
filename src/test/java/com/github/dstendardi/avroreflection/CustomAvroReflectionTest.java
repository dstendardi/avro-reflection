package com.github.dstendardi.avroreflection;

import com.viadeo.avro.reflection.DateCustomEncoding;
import com.viadeo.avro.reflection.NullableDateTimeEncoding;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.MyReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CustomAvroReflectionTest {

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
		private final WrappedGenericField<String> wrappedGenericField;

		public ClassWithOneUnmaterializedField(WrappedGenericField<String> wrappedGenericField) {
			this.wrappedGenericField = wrappedGenericField;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ClassWithOneUnmaterializedField that = (ClassWithOneUnmaterializedField) o;

			if (wrappedGenericField != null ? !wrappedGenericField.equals(that.wrappedGenericField) : that.wrappedGenericField != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return wrappedGenericField != null ? wrappedGenericField.hashCode() : 0;
		}
	}



	Schema BLOATED_YOLO_EVENT_SCHEMA = MY_REFLECT_DATA.getSchema(ClassWithOneUnmaterializedField.class);
	Schema SCHEMA = MY_REFLECT_DATA.getSchema(ClassExtendingAGenericClass.class);

	@Test
	public void schema_with_multiple_parametrized_fields() throws Exception {
		System.out.println(BLOATED_YOLO_EVENT_SCHEMA);
	}

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

		ReflectDatumWriter<ClassWithOneUnmaterializedField> writer = (ReflectDatumWriter<ClassWithOneUnmaterializedField>) MY_REFLECT_DATA.createDatumWriter(BLOATED_YOLO_EVENT_SCHEMA);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().jsonEncoder(BLOATED_YOLO_EVENT_SCHEMA, os);
		writer.write(new ClassWithOneUnmaterializedField(new WrappedGenericField<String>("optionalField")), encoder);
		encoder.flush();
		os.flush();


		assertEquals("{\"wrappedGenericField\":{\"com.github.dstendardi.avroreflection.CustomAvroReflectionTest$.WrappedGenericField\":{\"wrappedGenericField\":{\"string\":\"optionalField\"}}}}", os.toString("UTF-8"));
	}

	@Test
	public void deserialization_with_parametrized_type() throws Exception {

		ReflectDatumReader<ClassWithOneUnmaterializedField> reader = (ReflectDatumReader<ClassWithOneUnmaterializedField>) MY_REFLECT_DATA.createDatumReader(BLOATED_YOLO_EVENT_SCHEMA);
		ClassWithOneUnmaterializedField actual = reader.read(null, DecoderFactory.get().jsonDecoder(BLOATED_YOLO_EVENT_SCHEMA, "{\"wrappedGenericField\":{\"com.github.dstendardi.avroreflection.CustomAvroReflectionTest$.WrappedGenericField\":{\"wrappedGenericField\":{\"string\":\"optionalField\"}}}}"));

		assertEquals(new ClassWithOneUnmaterializedField(new WrappedGenericField<String>("optionalField")), actual);
	}

}

