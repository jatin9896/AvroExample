import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class Deserialize {
    public static void deserialiseData() {
        try {
            Schema schema = new Schema.Parser().parse(new File("src/employee.avsc"));
            DatumReader<GenericRecord> dr = new GenericDatumReader<GenericRecord>(schema);
            DataFileReader<GenericRecord> dfr = new DataFileReader<GenericRecord>(new File("src/data.txt"), dr);
            GenericRecord emp = null;
            while (dfr.hasNext()) {
                emp = dfr.next(emp);
                System.out.println(emp);
            }

        } catch (IOException e) {
            System.out.println("Exception " + e.getMessage());
        }
    }
}
