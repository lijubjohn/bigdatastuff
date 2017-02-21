package serialization;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import example.avro.User;

/**
 * Created by liju on 2/16/17.
 *
 * Example showing avro serialization and deserialization
 *
 * Run the maven generate-sources to create User pojo from avro schema
 */
public class AvroSerialization {

    public static void main(String[] args) {
        serialize();
        deserialize();
    }

    /**
     * Method to serialize
     */
    private static void serialize() {
        try {
            // Serialize user1, user2 etc to disk
            DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
            DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            User user1 = User.newBuilder()
                    .setName("John")
                    .setFavoriteColor("blue")
                    .setFavoriteNumber(123).build();

            User user2 = User.newBuilder()
                    .setName("Mark")
                    .setFavoriteColor("Red")
                    .setFavoriteNumber(null)
                    .build();

            // create the avro file for serialization
            dataFileWriter.create(user1.getSchema(), new File("users.avro"));

            // append all the users
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);

            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to deserialize
     */
    private static void deserialize() {
        try {
            // Deserialize Users from disk
            DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
            DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("users.avro"), userDatumReader);
            User user = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                user = dataFileReader.next(user);
                System.out.println("deserialized : "+user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
