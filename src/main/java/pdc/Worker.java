package pdc;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

/**
 * A Worker is a node in the cluster capable of running tasks.
 */
public class Worker {
    private String workerId;
    private Socket masterSocket;
    private InputStream input;
    private OutputStream output;
    private volatile boolean running = true;
    
    /**
     * Connects to the Master and initiates the connection.
     * The handshake must exchange 'Identifiers' between the master and worker.
     */
    public void joinCluster(String masterUrl, int port) {
        this.workerId = System.getenv().getOrDefault("WORKER_ID", 
                          "worker-" + UUID.randomUUID().toString().substring(0, 4));
        
        System.out.println("Worker " + workerId + " joining cluster at " + masterUrl + ":" + port);
        
        try {
            // Connect to master
            masterSocket = new Socket(masterUrl, port);
            input = masterSocket.getInputStream();
            output = masterSocket.getOutputStream();
            
            // Send registration
            Message regMsg = new Message("REGISTER_WORKER", workerId, new byte[0]);
            output.write(regMsg.pack());
            output.flush();
            
            System.out.println("Registration sent");
            
            // Start heartbeat thread
            startHeartbeats();
            
        } catch (IOException e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
        }
    }
    
    /**
     * Executes a received task block.
     */
    @SuppressWarnings("unused")
    public void execute() {
        System.out.println("Worker " + workerId + " executing task...");
        
        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while (running && (bytesRead = input.read(buffer)) != -1) {
                if (bytesRead > 0) {
                    Message message = Message.unpack(buffer);
                    
                    if ("RPC_REQUEST".equals(message.type)) {
                        // Simulate work
                        Thread.sleep(1000);
                        
                        // Send response
                        Message response = new Message("TASK_COMPLETE", workerId, "DONE".getBytes());
                        output.write(response.pack());
                        output.flush();
                        System.out.println("Task completed");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void startHeartbeats() {
        Thread heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(5000);
                    if (running && output != null) {
                        Message heartbeat = new Message("HEARTBEAT", workerId, new byte[0]);
                        output.write(heartbeat.pack());
                        output.flush();
                    }
                } catch (Exception e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: Worker <masterHost> <masterPort>");
            return;
        }
        
        Worker worker = new Worker();
        worker.joinCluster(args[0], Integer.parseInt(args[1]));
        worker.execute();
    }
}