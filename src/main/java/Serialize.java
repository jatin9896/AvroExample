import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Serialize {
    public static void serializeData() {
        try {
            Schema schema = new Schema.Parser().parse(new File("src/employee.avsc"));
            GenericRecord empRecord = new GenericData.Record(schema);
            empRecord.put("Name", "jatin");
            empRecord.put("age", 21);
            DatumWriter<GenericRecord> dw = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dfw = new DataFileWriter<GenericRecord>(dw);
            System.out.println("Serialize success");
            System.out.println("record entered " + empRecord);
            Socket client = new Socket("localhost", 8090);
            System.out.println("Just connected to " + client.getRemoteSocketAddress());
            OutputStream outToServer = client.getOutputStream();
            dfw.create(schema, outToServer);
            EncoderFactory  enc=new EncoderFactory();
            BinaryEncoder binaryEncoder=enc.binaryEncoder(outToServer,null);
            dw.write(empRecord,binaryEncoder);
            binaryEncoder.flush();
            dfw.flush();
            dfw.close();

        } catch (IOException e) {
            System.out.println("Exception " + e.getMessage());
        }
    }
}
