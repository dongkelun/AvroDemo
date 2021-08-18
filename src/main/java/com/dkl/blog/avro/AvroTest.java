package com.dkl.blog.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Created by dongkelun on 2021/8/17 19:35
 */
public class AvroTest {
    public static void main(String[] args) throws IOException {
        // Schema
        String schemaDescription = " {    \n"
                + " \"name\": \"FacebookUser\", \n"
                + " \"type\": \"record\",\n" + " \"fields\": [\n"
                + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                + "   {\"name\": \"name1\", \"type\": \"string\"},\n"
                + "   {\"name\": \"num_likes\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_photos\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_groups\", \"type\": \"int\"} ]\n" + "}";

        Schema s = Schema.parse(schemaDescription);    //parse方法在当前的Avro版本下已不推荐使用
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericDatumWriter w = new GenericDatumWriter(s);

        // Populate data
        GenericRecord r = new GenericData.Record(s);
        r.put("name", new org.apache.avro.util.Utf8("kazaff"));
        r.put("name1", "kazaff");
        r.put("num_likes", 1);
        r.put("num_groups", 423);
        r.put("num_photos", 0);

        // Encode
        w.write(r, e);
        e.flush();

        byte[] encodedByteArray = outputStream.toByteArray();
        String encodedString = outputStream.toString();

        System.out.println("encodedString: "+encodedString);

        // Decode using same schema
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(s);
        Decoder decoder = DecoderFactory.get().binaryDecoder(encodedByteArray, null);
        GenericRecord result = reader.read(null, decoder);
        System.out.println(result.get("name").toString());
        System.out.println(result.get("name1").toString());
        System.out.println(result.get("num_likes").toString());
        System.out.println(result.get("num_groups").toString());
        System.out.println(result.get("num_photos").toString());

    }
}
