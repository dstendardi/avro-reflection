package com.github.dstendardi.avroreflection;


import com.viadeo.avro.reflection.NullableDateTimeEncoding;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.MyReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroReflectionTest {

    public static final MyReflectData MY_REFLECT_DATA = MyReflectData.get();
    private BloatedEvent event = new BloatedEvent(new DateTime("2012-10-12"));

    public static class BloatedEvent {

        @AvroEncode(using = NullableDateTimeEncoding.class)
        private DateTime dateTime;

        public BloatedEvent() {
        }

        public BloatedEvent(DateTime dateTime) {
            this.dateTime = dateTime;
        }

        public DateTime getDateTime() {
            return dateTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BloatedEvent that = (BloatedEvent) o;

            if (dateTime != null ? !dateTime.equals(that.dateTime) : that.dateTime != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return dateTime != null ? dateTime.hashCode() : 0;
        }
    }

    Schema SCHEMA = MY_REFLECT_DATA.getSchema(BloatedEvent.class);

    @Test
    public void schema_with_custom_encoder() throws Exception {

        assertNotNull(SCHEMA.getField("dateTime"));
        Schema dateTimeSchema = SCHEMA.getField("dateTime").schema().getTypes().get(1);
        Schema.Type dateTime = dateTimeSchema.getType();
        assertEquals(dateTime, Schema.Type.STRING);
        assertEquals("inst", dateTimeSchema.getProp("tag"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serialization_with_custom_encoder() throws Exception {
        ReflectDatumWriter<BloatedEvent> writer = (ReflectDatumWriter<BloatedEvent>) MY_REFLECT_DATA.createDatumWriter(SCHEMA);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(SCHEMA, os);
        writer.write(event, encoder);
        encoder.flush();
        os.flush();

        assertEquals("{\"dateTime\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"}}", os.toString("UTF-8"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void deserialization_with_custom_encoder() throws IOException {

        ReflectDatumReader<BloatedEvent> reader = (ReflectDatumReader<BloatedEvent>) MY_REFLECT_DATA.createDatumReader(SCHEMA);
        BloatedEvent actual = reader.read(null, DecoderFactory.get().jsonDecoder(SCHEMA, "{\"dateTime\":{\"string\":\"2012-10-12T00:00:00.000+02:00\"}}"));

        assertEquals(event, actual);
    }

}
