import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Created by liju on 6/6/17.
 */
public class ReadAvroFile {
    public static void main(String[] args) throws IOException {

        final Schema schema = new Schema.Parser().parse(new File("/Users/liju/Documents/gitprojects/bigdatastuff/avro-stuff/src/main/resources/student.avsc"));

        File file = new File("/Users/liju/Documents/gitprojects/bigdatastuff/avro-stuff/src/main/resources/student.avro");
        try {
            //read the avro file to GenericRecord
            final GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(schema);
            final DataFileReader<GenericRecord> genericRecords = new DataFileReader<>(file, genericDatumReader);

            //serialize GenericRecords
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);

            Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);

            while (genericRecords.hasNext()) {
                writer.write(genericRecords.next(), binaryEncoder);
            }
            binaryEncoder.flush();
            out.close();

            //deserialize
            DecoderFactory decoderFactory = DecoderFactory.get();
            GenericDatumReader<GenericRecord> genericDatumReader2 = new GenericDatumReader<>(schema);
            final GenericRecord genericRecord = genericDatumReader2.read(null, decoderFactory.binaryDecoder(out.toByteArray(), null));


            //write to file
            File fileOut = new File("/Users/liju/Documents/gitprojects/bigdatastuff/avro-stuff/src/main/resources/student_out.avro");
            final GenericDatumWriter<GenericRecord> genericRecordGenericDatumWriter = new GenericDatumWriter<>(schema);
            final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(genericRecordGenericDatumWriter);
            dataFileWriter.create(schema,fileOut);
            dataFileWriter.append(genericRecord);
            dataFileWriter.close();


        } catch (Exception e) {
             e.printStackTrace();
        }
    }
}
