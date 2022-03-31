package Bigdata.MessageReceiver;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.text.DecimalFormat;
import java.util.Random;

import org.json.JSONObject;

public class Client {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
       /* if (args.length < 3) {
            System.out.println("Syntax: client <service_name> <RAM> <Disk>");
            return;
        }
        String serviceName = args[1];
        int RAM = Integer.parseInt(args[2]);
        int Disk = Integer.parseInt(args[3]);
 */

        String serviceName = "Health";
        int RAM = 4;
        int Disk = 100;
        try {
            InetAddress address = InetAddress.getByName("localhost");
            System.out.println(address.getHostAddress());
            DatagramSocket socket = new DatagramSocket();

            while (true) {

                JSONObject massage = new JSONObject();
                massage.put("serviceName", serviceName);

                Long Timestamp = System.currentTimeMillis();
                massage.put("Timestamp", Timestamp);

                DecimalFormat formatter = new DecimalFormat("#0.00");  // edited here.
                double randomDbl = Math.random();
                String c = formatter.format(randomDbl);
                double CPU = Double.parseDouble(c);
                massage.put("CPU", CPU);

                JSONObject ram_massage = new JSONObject();
                ram_massage.put("Total", RAM);
                Random rand = new Random();
                int random_integer = rand.nextInt(RAM-0) + 0;
                ram_massage.put("Free", random_integer);
                massage.put("RAM", ram_massage);


                JSONObject disk_massage = new JSONObject();
                disk_massage.put("Total", Disk);
                int random_integer2 = rand.nextInt(Disk-0) + 0;
                disk_massage.put("Free", random_integer2);
                massage.put("Disk", disk_massage);


                System.out.println(massage.toString());

                byte buf[] = null;
                buf = massage.toString().getBytes();


                DatagramPacket DpSend =
                        new DatagramPacket(buf, buf.length, address, 3500);

                socket.send(DpSend);

                Thread.sleep(10);
            }

        } catch (SocketTimeoutException ex) {
            System.out.println("Timeout error: " + ex.getMessage());
            ex.printStackTrace();
        } catch (IOException ex) {
            System.out.println("Client error: " + ex.getMessage());
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}