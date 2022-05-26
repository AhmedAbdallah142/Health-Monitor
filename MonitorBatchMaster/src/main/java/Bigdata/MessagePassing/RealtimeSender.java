package Bigdata.MessagePassing;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class RealtimeSender{
    static final Object l = new Object();
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private final StringBuilder messages = new StringBuilder();
    private final int port;

    public RealtimeSender (int port) {
        this.port = port;
        start();
    }

    public void start() {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(port);
                while (!serverSocket.isClosed()) {
                    clientSocket = serverSocket.accept();
                    System.out.println("[+] Client accepted");

                    synchronized (l) {
                        while(!clientSocket.isClosed()) {
                            if (messages.length() == 0)
                                l.wait();
                            out = new PrintWriter(clientSocket.getOutputStream(), true);
                            out.print(messages);
                            messages.delete(0, messages.length());
                            out.close();
                        }
                    }

                    clientSocket.close();
                    System.out.println("[+] Connection with client ended");
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    public void sendMessage(String message) {
        synchronized (l) {
            messages.append(message).append("\n");
            l.notify();
        }
    }

    public static void main(String[] args) {
        RealtimeSender server = new RealtimeSender(9999);
        server.sendMessage("Service1,1646485461,0.2,4,1.5,100,75");

//        String msg = "";
//        while (!msg.equals("end")) {
//            Scanner s = new Scanner(System.in);
//            msg = s.next();
//            System.out.println(msg);
//            server.sendMessage(msg);
//        }
    }
}
/*
ServiceName,Timestamp,CPU_utl,TotalRAM,FreeRAM,TotalDisk,FreeDisk
0.2,Service1,1646485461,4,1.5,100,75
 */