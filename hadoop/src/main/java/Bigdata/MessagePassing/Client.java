package Bigdata.MessagePassing;

import org.json.JSONObject;

import java.io.File;

import java.io.FileReader;
import java.net.*;


public class Client {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Receiver r = new Receiver();
        try {
            InetAddress address = InetAddress.getByName("localhost");
            System.out.println(address.getHostAddress());
            DatagramSocket socket = new DatagramSocket();

            int n = 141;
            for(int i = 0 ; i < n ; i++) {
//                File myObj = new File("data/health_"+(i)+".json");
                File myObj = new File("input/data.log");
                String object = "";
                FileReader fr = new FileReader(myObj);
                int content;
                while ((content = fr.read()) != -1) {
                    object += (char) content;
                    if (object.length() > 3 && object.contains("}{")) {
                        object = object.substring(0, object.length() - 1);
                        JSONObject massage = new JSONObject(object);
//                        System.out.println(massage.toString());
                        byte buf[] = null;
                        buf = massage.toString().getBytes();
//                        DatagramPacket DpSend =
//                                new DatagramPacket(buf, buf.length, address, 3500);
//                        socket.send(DpSend);
                        r.handleReceived(buf);
                        object = "{";
//                        Thread.sleep(0);
                    }
                }
//                System.out.println("\n\n\n\n\n\n\n\n\n"+i+"\n\n\n\n\n\n\n\n\n");
//                Thread.sleep(5);
            }
        } catch (SocketTimeoutException ex) {
            System.out.println("Timeout error: " + ex.getMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            System.out.println("Client error: " + ex.getMessage());
            ex.printStackTrace();
        }

    }
}