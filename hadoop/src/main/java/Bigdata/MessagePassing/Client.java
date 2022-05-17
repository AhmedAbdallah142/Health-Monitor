package Bigdata.MessagePassing;

import org.json.JSONObject;

import java.io.File;

import java.io.FileReader;
import java.net.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;


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

            int n = 1;
            for(int i = 0 ; i < n ; i++) {
//                File myObj = new File("data/health_"+(i)+".json");
                File myObj = new File("health_141.json");
                StringBuilder object = new StringBuilder();
                FileReader fr = new FileReader(myObj);
                int content;
                while ((content = fr.read()) != -1) {
                    object.append((char) content);
                    if (object.length() > 3 && object.toString().contains("}{")) {
                        object = new StringBuilder(object.substring(0, object.length() - 1));
                        JSONObject massage = new JSONObject(object.toString());
//                        System.out.println(massage.toString());
                        byte[] buf = null;

                        long date = massage.getLong("Timestamp");
                        long t_current = System.currentTimeMillis();
//                        if(date < t_current/1000){
                            buf = massage.toString().getBytes();
//                        DatagramPacket DpSend =
//                                new DatagramPacket(buf, buf.length, address, 3500);
//                        socket.send(DpSend);
                            r.handleReceived(buf);
//                        }
                        object = new StringBuilder("{");
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