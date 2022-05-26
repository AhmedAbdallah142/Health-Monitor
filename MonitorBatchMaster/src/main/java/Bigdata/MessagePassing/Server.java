package Bigdata.MessagePassing;

import Bigdata.monitor.FileMonitor.HadoopFileOperation;
import Bigdata.monitor.TimeMonitor;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class Server {
    final private static int maxMessages = 128;//25000;
    RealtimeSender server;
    private String currDate;
    private int messagesInBuffer = 0;
    //    private final JSONArray messagesBuffer;
    private final StringBuilder messagesBuffer;
    // hdfs time
    private double hdfsWriteTimeAvg = 0;
    private long n_batches = 0L;

    // throughput
    private long startTime = 0L;

    // average end to end time
    private double endToEndDelayAvg = 0;
    private long rcvTimeSum = 0L;

    public Server() {
        currDate = TimeMonitor.getDate();
        messagesBuffer = new StringBuilder();
        server = new RealtimeSender(9999);
    }

    public void runServer(int port) throws IOException {
        DatagramSocket socket = new DatagramSocket(port);
        DatagramPacket packet;
        byte[] received = new byte[65535];

        System.out.println("[+] Server is running at port " + port);
        startTime = System.nanoTime();
        while (true) {
            packet = new DatagramPacket(received, received.length);
            socket.receive(packet);

            rcvTimeSum += System.nanoTime();
            handleReceived(received);

            Arrays.fill(received, 0, packet.getLength(), (byte) 0);
        }
    }

    public void handleReceived(byte[] data) throws IOException {
        if (!TimeMonitor.getDate().equalsIgnoreCase(currDate)) {
            sendBatch();
            currDate = TimeMonitor.getDate();
        }

        int JSON_start = 0, JSON_end;
        // read messages one by one
        while ((JSON_end = matchJSON(data, JSON_start)) != -1) {
            // parse to JSON Object

            byte[] message = Arrays.copyOfRange(data, JSON_start, JSON_end);

            // catch if parse failed
            try {
//                messagesBuffer.put(new JSONObject(new String(message)));
                String realTimeMessage = Convert(new JSONObject(new String(message)));
//                System.out.println(realTimeMessage);
                server.sendMessage(realTimeMessage);
                messagesBuffer.append(realTimeMessage).append("\n");
                ++messagesInBuffer;

//                if(messagesInBuffer % 128 == 0) {
//                    double throughput = ((double) messagesInBuffer / (System.nanoTime() - startTime)) * 1e9;
//                    System.out.println(
//                            String.format("[+] Number of messages in buffer = %d \n", messagesInBuffer) +
//                                    String.format("[+] Current throughput = %.2f records/sec", throughput)
//                    );
//                }

                JSON_start = JSON_end;
            } catch (RuntimeException e) {
                System.out.println("Failed to save message:\n" + e.getMessage());
                e.printStackTrace();
                break;
            }

            if (messagesInBuffer >= maxMessages)
                sendBatch();
        }
    }

    private int matchJSON(byte[] msg, int start) {
        if (start >= msg.length || msg[start] != '{')
            return -1;

        int i = start + 1, bracketCount = 1;
        while (i < msg.length && bracketCount > 0) {
            if (msg[i] == '{')
                ++bracketCount;
            else if (msg[i] == '}')
                --bracketCount;
            else if (msg[i] == 0)
                return -1;
            ++i;
        }

        return bracketCount == 0 ? i : -1;
    }

    private void sendBatch() throws IOException {
        long tec = System.nanoTime();
        HadoopFileOperation file = new HadoopFileOperation();
        String hdfsFilePath = "hdfs://localhost:9000/Logs/" + "20220527" + ".csv";
        System.out.println(file.AddLogFile(messagesBuffer.toString(), hdfsFilePath));

//        file.ReadFile(fileSystem,hdfsFilePath);
        file.closeFileSystem();
//        try {
//            Thread.sleep((int) (1000 * Math.random()));
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        long toc = System.nanoTime();

        double recPerNano = (double) messagesInBuffer / (toc - tec);
        hdfsWriteTimeAvg = (recPerNano + n_batches * hdfsWriteTimeAvg) / (n_batches + 1);

        double endToEndDelayBatch = (System.nanoTime() - (float) rcvTimeSum / messagesInBuffer) / 1e9;
        endToEndDelayAvg = (endToEndDelayBatch + n_batches * endToEndDelayAvg) / (n_batches + 1);

        System.out.println(
                "********************* Batch Sent *********************\n" +
                        String.format("[+] Wrote %d messages in %.2f ms\n", messagesInBuffer, (toc - tec) / 1e6) +
                        String.format("[+] Average hdfs write speed = %.2f records/sec\n", recPerNano * 1e9) +
                        String.format("[+] Average end to end delay = %.2f sec\n", endToEndDelayAvg) +
                        "******************************************************"
        );

        ++n_batches;

        // reset
        messagesBuffer.delete(0, messagesBuffer.length());
//        messagesBuffer.clear();
        messagesInBuffer = 0;
        rcvTimeSum = 0L;
        startTime = System.nanoTime();
    }

    public static String getDay(long timeStamp) {
        timeStamp *= 1000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return simpleDateFormat.format(timeStamp);
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        Server r = new Server();
        r.runServer(3500);
    }

    public static String Convert(JSONObject obj){
        JSONObject NestedRAM= (JSONObject) obj.get("RAM");
        JSONArray messagesBuffer1= new JSONArray();
        messagesBuffer1.put(NestedRAM);
        String value1=CDL.toString(obj.getJSONObject("Disk").names(),messagesBuffer1);
        value1 = value1.substring(0,value1.length()-1);
        obj.put("RAM",value1);
        JSONObject NestedDisk= (JSONObject) obj.get("Disk");
        JSONArray messagesBuffer= new JSONArray();
        messagesBuffer.put(NestedDisk);
        String value2=CDL.toString(obj.getJSONObject("Disk").names(),messagesBuffer);
        value2 = value2.substring(0,value2.length()-1);
        obj.put("Disk",value2);
        StringBuilder Str = new StringBuilder();
        Str.append(obj.get("CPU")+",");
        Str.append(obj.get("serviceName")+",");
        Str.append(obj.get("Timestamp")+",");
        Str.append(value1+",");
        Str.append(value2);
        System.out.println(Str.toString());
        return Str.toString();
    }
}