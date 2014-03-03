package com.github.dstendardi.avroreflection;

import com.viadeo.avro.reflection.DateCustomEncoder;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroReflectionTest {

    private BloatedEvent event = new BloatedEvent(new DateTime("2012-10-12"));

    public static class BloatedEvent {

        @AvroEncode(using = DateCustomEncoder.class)
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

    Schema schema = ReflectData.get().getSchema(BloatedEvent.class);


    @Test
    public void schema_with_custom_encoder() throws Exception {
        assertNotNull(schema.getField("dateTime"));
        Schema dateTimeSchema = schema.getField("dateTime").schema();
        Schema.Type dateTime = dateTimeSchema.getType();
        assertEquals(dateTime, Schema.Type.STRING);
        assertEquals("inst", dateTimeSchema.getProp("tag"));
    }

    @Test
    public void serialization_with_custom_encoder() throws Exception {
        ReflectDatumWriter<BloatedEvent> writer = new ReflectDatumWriter<BloatedEvent>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
        writer.write(event, encoder);
        encoder.flush();
        os.flush();

        assertEquals("{\"dateTime\":\"2012-10-12T00:00:00.000+02:00\"}", os.toString("UTF-8"));
    }

    @Test
    public void deserialization_with_custom_encoder() throws IOException {

        ReflectDatumReader<BloatedEvent> reader = new ReflectDatumReader<BloatedEvent>(schema);

        BloatedEvent actual = reader.read(null, DecoderFactory.get().jsonDecoder(schema, "{\"dateTime\":\"2012-10-12T00:00:00.000+02:00\"}"));

        assertEquals(event, actual);
    }

}
