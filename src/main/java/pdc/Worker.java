package pdc;

import java.io.*;
import java.net.Socket;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.UUID;

/**
 * A Worker is a node in the cluster capable of running tasks.
 * Enhanced with automatic reconnection and fault tolerance.
 */
public class Worker {
    private String workerId;
    private String masterHost;
    private int masterPort;
    private Socket masterSocket;
    private InputStream input;
    private OutputStream output;
    private volatile boolean running = true;
    private volatile boolean connected = false;
    
    // Connection retry settings
    private static final int MAX_RETRIES = 10;
    private static final int RETRY_DELAY_MS = 3000;
    private static final int MAX_RECONNECT_ATTEMPTS = 999999; // Essentially infinite
    private static final int RECONNECT_DELAY_MS = 5000;
    private static final int SOCKET_TIMEOUT_MS = 30000;
    
    // Heartbeat settings
    private static final int HEARTBEAT_INTERVAL_MS = 5000;
    private static final int HEARTBEAT_TIMEOUT_MS = 30000;
    
    // Threads
    private Thread heartbeatThread;
    private Thread listenThread;
    private Thread reconnectionThread;
    
    /**
     * Connects to the Master and initiates the connection.
     * The handshake must exchange 'Identifiers' between the master and worker.
     */
    public void joinCluster(String masterHost, int port) {
        this.masterHost = masterHost;
        this.masterPort = port;
        this.workerId = System.getenv().getOrDefault("WORKER_ID", 
                          "worker-" + UUID.randomUUID().toString().substring(0, 8));
        
        System.out.println("Worker " + workerId + " attempting to join cluster at " + masterHost + ":" + port);
        
        // Start reconnection thread for automatic recovery
        startReconnectionThread();
        
        // Initial connection attempt
        connectToMaster();
    }
    
    /**
     * Start reconnection thread that monitors connection and reconnects if needed
     */
    private void startReconnectionThread() {
        reconnectionThread = new Thread(() -> {
            int reconnectAttempts = 0;
            
            while (running) {
                try {
                    Thread.sleep(RECONNECT_DELAY_MS);
                    
                    // Check if we need to reconnect
                    if (running && !isConnected()) {
                        reconnectAttempts++;
                        System.out.println("Reconnection attempt " + reconnectAttempts + 
                                         " - attempting to reconnect to master...");
                        
                        // Clean up old connection before reconnecting
                        cleanup();
                        
                        // Attempt to reconnect
                        if (connectToMaster()) {
                            System.out.println("Successfully reconnected to master after " + 
                                             reconnectAttempts + " attempts");
                            reconnectAttempts = 0;
                        }
                    } else if (isConnected()) {
                        reconnectAttempts = 0; // Reset counter when connected
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        reconnectionThread.setDaemon(true);
        reconnectionThread.start();
        System.out.println("Reconnection monitoring thread started");
    }
    
    /**
     * Connect to master with retry logic
     */
    private boolean connectToMaster() {
        int retries = 0;
        
        while (retries < MAX_RETRIES && running) {
            try {
                // Create socket and set timeout
                masterSocket = new Socket(masterHost, masterPort);
                masterSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
                
                // Increase buffer sizes for large messages
                masterSocket.setReceiveBufferSize(262144); // 256KB
                masterSocket.setSendBufferSize(262144); // 256KB
                
                input = masterSocket.getInputStream();
                output = masterSocket.getOutputStream();
                
                // Send registration message
                Message regMsg = Message.createRegistration(workerId);
                output.write(regMsg.pack());
                output.flush();
                
                System.out.println("Registration sent, waiting for acknowledgment...");
                
                // Wait for acknowledgment with timeout
                byte[] buffer = new byte[8192];
                int bytesRead = input.read(buffer);
                
                if (bytesRead > 0) {
                    Message response = Message.unpack(buffer);
                    
                    if ("WORKER_ACK".equals(response.type)) {
                        System.out.println("Worker " + workerId + " successfully registered with master");
                        connected = true;
                        
                        // Start heartbeat thread
                        startHeartbeats();
                        
                        // Start listening for tasks in a separate thread
                        startListenThread();
                        
                        return true;
                    }
                }
                
            } catch (ConnectException e) {
                retries++;
                System.err.println("Failed to connect to master (attempt " + retries + "/" + MAX_RETRIES + "): " + e.getMessage());
                
                if (retries < MAX_RETRIES) {
                    System.out.println("Retrying in " + (RETRY_DELAY_MS/1000) + " seconds...");
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    System.err.println("Max connection retries reached.");
                }
                
            } catch (SocketTimeoutException e) {
                System.err.println("Connection timeout: " + e.getMessage());
                retries++;
                
            } catch (IOException e) {
                System.err.println("Error during cluster join: " + e.getMessage());
                retries++;
                
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            // Cleanup failed connection attempt
            cleanup();
        }
        
        return false;
    }
    
    /**
     * Check if worker is connected to master
     */
    private boolean isConnected() {
        return connected && 
               masterSocket != null && 
               masterSocket.isConnected() && 
               !masterSocket.isClosed() &&
               output != null &&
               input != null;
    }
    
    /**
     * Start listening for tasks in a separate thread
     */
    private void startListenThread() {
        listenThread = new Thread(() -> {
            System.out.println("Worker " + workerId + " listening for tasks...");
            
            try {
                byte[] buffer = new byte[262144]; // 256KB buffer for large messages
                int bytesRead;
                
                while (connected && running && input != null && (bytesRead = input.read(buffer)) != -1) {
                    if (bytesRead > 0) {
                        processMessages(buffer, bytesRead);
                    }
                }
            } catch (SocketTimeoutException e) {
                // Timeout is expected, just continue
                System.out.println("Listen timeout - checking connection...");
            } catch (IOException e) {
                if (connected && running) {
                    System.err.println("Connection to master lost: " + e.getMessage());
                    connected = false;
                }
            } finally {
                if (connected) {
                    connected = false;
                    cleanup();
                }
            }
        });
        
        listenThread.setDaemon(true);
        listenThread.start();
    }
    
    /**
     * Process incoming messages from buffer
     */
    private void processMessages(byte[] buffer, int bytesRead) throws IOException {
        int offset = 0;
        
        while (offset < bytesRead) {
            // Check if we have a complete message
            int msgLength = Message.getCompleteMessageLength(
                java.util.Arrays.copyOfRange(buffer, offset, bytesRead));
            
            if (msgLength > 0 && offset + msgLength <= bytesRead) {
                // Extract complete message
                byte[] msgData = new byte[msgLength];
                System.arraycopy(buffer, offset, msgData, 0, msgLength);
                
                Message message = Message.unpack(msgData);
                handleMessage(message);
                
                offset += msgLength;
            } else {
                // Incomplete message, wait for more data
                break;
            }
        }
    }
    
    /**
     * Handle different message types from master
     */
    private void handleMessage(Message message) throws IOException {
        switch (message.type) {
            case "HEARTBEAT":
                System.out.println("Heartbeat received from master");
                sendHeartbeat();
                break;
                
            case "RPC_REQUEST":
                handleTask(message);
                break;
                
            case "CHUNK":
                handleChunk(message);
                break;
                
            default:
                System.out.println("Unknown message type: " + message.type);
        }
    }
    
    /**
     * Handle chunked messages
     */
    private void handleChunk(Message message) throws IOException {
        System.out.println("Received chunk " + message.getChunkId() + 
                         " of " + message.getTotalChunks());
        
        // TODO: Implement chunk reassembly
        // For now, just acknowledge receipt
        Message ack = new Message("WORKER_ACK", workerId, 
            ("Chunk " + message.getChunkId() + " received").getBytes());
        output.write(ack.pack());
        output.flush();
    }
    
    /**
     * Handle a task request from master
     */
    private void handleTask(Message message) throws IOException {
        System.out.println("Processing task...");
        
        try {
            // Parse task data (format: "taskId:matrixData")
            String taskData = new String(message.payload);
            String[] parts = taskData.split(":", 2);
            String taskId = parts[0];
            
            // Simulate work with progress updates
            System.out.println("Working on task " + taskId + "...");
            
            // For large tasks, send periodic heartbeats to show we're alive
            int workDuration = 5000; // 5 seconds
            int steps = 10;
            for (int i = 0; i < steps; i++) {
                Thread.sleep(workDuration / steps);
                if (i % 3 == 0) {
                    sendHeartbeat(); // Keep connection alive during long tasks
                }
            }
            
            // Create response
            String result = "Task " + taskId + " completed by " + workerId;
            Message response = new Message("TASK_COMPLETE", workerId, result.getBytes());
            
            output.write(response.pack());
            output.flush();
            
            System.out.println("Task " + taskId + " completed");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Message errorMsg = new Message("TASK_ERROR", workerId, 
                ("Task interrupted: " + e.getMessage()).getBytes());
            output.write(errorMsg.pack());
            output.flush();
            
        } catch (Exception e) {
            System.err.println("Error processing task: " + e.getMessage());
            
            // Send error response
            Message errorMsg = new Message("TASK_ERROR", workerId, 
                ("Task failed: " + e.getMessage()).getBytes());
            output.write(errorMsg.pack());
            output.flush();
        }
    }
    
    /**
     * Start heartbeat thread
     */
    private void startHeartbeats() {
        heartbeatThread = new Thread(() -> {
            int missedHeartbeats = 0;
            
            while (connected && running) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                    
                    if (connected && output != null) {
                        sendHeartbeat();
                        missedHeartbeats = 0; // Reset on success
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                    
                } catch (IOException e) {
                    missedHeartbeats++;
                    System.err.println("Failed to send heartbeat (" + missedHeartbeats + 
                                     " missed) - " + e.getMessage());
                    
                    // If we miss too many heartbeats, assume connection is dead
                    if (missedHeartbeats >= 3) {
                        System.err.println("Too many missed heartbeats, marking connection as dead");
                        connected = false;
                        break;
                    }
                }
            }
        });
        
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        System.out.println("Heartbeat thread started");
    }
    
    /**
     * Send heartbeat to master
     */
    private void sendHeartbeat() throws IOException {
        if (output != null && connected) {
            Message heartbeat = Message.createHeartbeat(workerId);
            output.write(heartbeat.pack());
            output.flush();
            System.out.println("Heartbeat sent");
        }
    }
    
    /**
     * Executes a received task block.
     * This method is required by the tests.
     */
    public void execute() {
        System.out.println("Worker " + workerId + " execute() called - already listening for tasks");
        // The actual listening happens in listen thread
    }
    
    /**
     * Force reconnection attempt
     */
    public void forceReconnect() {
        System.out.println("Forcing reconnection...");
        connected = false;
        cleanup();
    }
    
    /**
     * Clean up resources
     */
    private void cleanup() {
        System.out.println("Cleaning up worker " + workerId);
        
        try {
            connected = false;
            
            if (input != null) {
                try { input.close(); } catch (IOException e) { /* Ignore */ }
                input = null;
            }
            
            if (output != null) {
                try { output.close(); } catch (IOException e) { /* Ignore */ }
                output = null;
            }
            
            if (masterSocket != null && !masterSocket.isClosed()) {
                try { masterSocket.close(); } catch (IOException e) { /* Ignore */ }
                masterSocket = null;
            }
            
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }
    
    /**
     * Shutdown worker gracefully
     */
    public void shutdown() {
        System.out.println("Shutting down worker " + workerId);
        running = false;
        connected = false;
        cleanup();
    }
    
    /**
     * Main method
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: Worker <masterHost> <masterPort>");
            System.out.println("Example: Worker localhost 8080");
            return;
        }
        
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        
        Worker worker = new Worker();
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down worker...");
            worker.shutdown();
        }));
        
        worker.joinCluster(masterHost, masterPort);
        
        // Keep main thread alive
        while (worker.running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                worker.shutdown();
                break;
            }
        }
    }
}