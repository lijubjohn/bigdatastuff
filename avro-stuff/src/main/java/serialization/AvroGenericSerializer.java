package serialization;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Created by liju on 2/17/17.
 *
 * This example shows how to serialize & deserialize using GerenicDatumWriter.
 * Here we no need to generate pojos from schema to serialize or deserialize
 */
public class AvroGenericSerializer {

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    /**
     *
     * @param schema - schema of the object
     * @param genericArray  - top level element type corresponding to schema
     *         This can be genericArray or GenericRecord as per schema
     * @return
     */
    public byte[] serialize(Schema schema, GenericArray genericArray) {
        try {
            GenericDatumWriter<GenericArray> writer = new GenericDatumWriter<>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder binaryEncoder = encoderFactory.binaryEncoder(out, null);
            writer.write(genericArray, binaryEncoder);
            binaryEncoder.flush();
            out.close();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("serialization failed", e);
        }
    }

    public GenericArray deserialize(Schema schema, byte[] array) {
        try {
            GenericDatumReader<GenericArray> genericDatumReader = new GenericDatumReader<>(schema);
            final GenericArray genericArray = genericDatumReader.read(null, decoderFactory.binaryDecoder(array, null));
            return genericArray;
        } catch (Exception e) {
            throw new RuntimeException("deserialization failed", e);
        }
    }


    public static void main(String[] args) {
        AvroGenericSerializer genericSerializer = new AvroGenericSerializer();
        String schema  = "{\"namespace\":\"com.test\",\"name\":\"users\",\"type\":\"array\",\"items\":{\"name\":\"user\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"gender\",\"type\":\"string\"},{\"name\":\"hgt\",\"type\":\"float\"}]}}";
        final Schema avroSchema = new Schema.Parser().parse(schema);

        User user1  = new User("mark","male",6.1f);
        User user2 = new User("Sara","female",5.2f);
        List<User> users = new ArrayList<>();
        users.add(user1);
        users.add(user2);

        GenericArray genericArray = createGenericArray(users,avroSchema);

        final byte[] serialized = genericSerializer.serialize(avroSchema, genericArray);

        final GenericArray deserialized = genericSerializer.deserialize(avroSchema, serialized);

        assert genericArray.equals(deserialized);

        System.out.println("before serialization  : "+genericArray.toString());
        System.out.println("after deserialization : " + deserialized.toString());
    }

    private static GenericArray createGenericArray(List<User> users,Schema schema) {

        GenericArray<GenericRecord> avroArray = new GenericData.Array<>(users.size(), schema );

        for (User user : users) {
            GenericRecord genericRecord = new GenericData.Record(schema.getElementType());
            genericRecord.put("name",user.name);
            genericRecord.put("gender",user.gender);
            genericRecord.put("hgt",user.hgt);
            avroArray.add(genericRecord);
        }
        return avroArray;
    }

}

final class User {
    String name;
    String gender;
    float hgt;

    public User(String name, String gender, float hgt) {
        this.name = name;
        this.gender = gender;
        this.hgt = hgt;
    }
}
