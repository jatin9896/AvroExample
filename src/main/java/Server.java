import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class Server extends Thread {
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(8090);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(true) {
            try {
                System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
                Socket server = serverSocket.accept();

                System.out.println("Just connected to " + server.getRemoteSocketAddress());
                Schema schema = new Schema.Parser().parse(new File("src/employee.avsc"));
                DatumReader<GenericRecord> dr = new GenericDatumReader<GenericRecord>(schema);
                GenericRecord emp = null;
                InputStream in=server.getInputStream();
                DecoderFactory decoderFactory = new DecoderFactory();
                BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(in, null);
                dr.read(emp,binaryDecoder);
                System.out.println("Received: " + emp.toString());
              //  System.out.println(in.read());
                DataOutputStream out = new DataOutputStream(server.getOutputStream());
                out.writeUTF("Thank you for connecting to " + server.getLocalSocketAddress() + "\nGoodbye!");
                server.close();

            }catch(SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            }catch(IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }
    public static void main(String args[]) {

            Thread t = new Server();
            t.start();

    }
}
