package com.dkl.blog.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * Created by dongkelun on 2021/8/17 19:41
 */
public class AvroDataFile {
    public static void main(String[] args) throws IOException {
        // Schema
        String schemaDescription = " {    \n"
                + " \"name\": \"FacebookUser\", \n"
                + " \"type\": \"record\",\n" + " \"fields\": [\n"
                + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                + "   {\"name\": \"num_likes\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_photos\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_groups\", \"type\": \"int\"} ]\n" + "}";

        Schema schema = Schema.parse(schemaDescription);    //parse方法在当前的Avro版本下已不推荐使用
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, os);

        // Populate data
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("name", new org.apache.avro.util.Utf8("kazaff"));
        datum.put("num_likes", 1);
        datum.put("num_groups", 423);
        datum.put("num_photos", 0);

        dataFileWriter.append(datum);
        dataFileWriter.close();

        System.out.println("encoded string: " + os.toString());    //可以看到，数据是头部携带了schema metadata

        // Decode
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(is, reader);

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            System.out.println(record.getSchema());    //可以直接获取该数据的json schema定义
            System.out.println(record.get("name").toString());
            System.out.println(record.get("num_likes").toString());
            System.out.println(record.get("num_groups").toString());
            System.out.println(record.get("num_photos").toString());
        }
    }
}
