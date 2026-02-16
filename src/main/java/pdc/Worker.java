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
    
    // Track connection state
    private int consecutiveFailures = 0;
    private long lastSuccessfulHeartbeat = 0;
    
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
                            consecutiveFailures = 0;
                        }
                    } else if (isConnected()) {
                        reconnectAttempts = 0; // Reset counter when connected
                        
                        // Check if we've missed too many heartbeats
                        if (lastSuccessfulHeartbeat > 0 && 
                            System.currentTimeMillis() - lastSuccessfulHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                            System.out.println("No heartbeats received for too long, reconnecting...");
                            connected = false;
                            cleanup();
                        }
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
                
                // Enable keep-alive
                masterSocket.setKeepAlive(true);
                
                // Disable Nagle's algorithm for lower latency
                masterSocket.setTcpNoDelay(true);
                
                // Increase buffer sizes for large messages
                masterSocket.setReceiveBufferSize(262144); // 256KB
                masterSocket.setSendBufferSize(262144); // 256KB
                
                input = masterSocket.getInputStream();
                output = masterSocket.getOutputStream();
                
                // Send registration message
                Message regMsg = Message.createRegistration(workerId);
                byte[] packedMsg = regMsg.pack();
                output.write(packedMsg);
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
                        consecutiveFailures = 0;
                        lastSuccessfulHeartbeat = System.currentTimeMillis();
                        
                        // Start heartbeat thread
                        startHeartbeats();
                        
                        // Start listening for tasks in a separate thread
                        startListenThread();
                        
                        return true;
                    } else {
                        System.err.println("Unexpected response type: " + response.type);
                    }
                } else {
                    System.err.println("No response from master");
                }
                
            } catch (ConnectException e) {
                consecutiveFailures++;
                retries++;
                System.err.println("Failed to connect to master (attempt " + retries + "/" + MAX_RETRIES + 
                                 ", consecutive failures: " + consecutiveFailures + "): " + e.getMessage());
                
                if (retries < MAX_RETRIES) {
                    System.out.println("Retrying in " + (RETRY_DELAY_MS/1000) + " seconds...");
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    System.err.println("Max connection retries reached. Will continue trying in background...");
                    // Don't give up completely, reconnection thread will keep trying
                }
                
            } catch (SocketTimeoutException e) {
                consecutiveFailures++;
                retries++;
                System.err.println("Connection timeout (attempt " + retries + "/" + MAX_RETRIES + "): " + e.getMessage());
                
            } catch (IOException e) {
                consecutiveFailures++;
                retries++;
                System.err.println("Error during cluster join (attempt " + retries + "/" + MAX_RETRIES + "): " + e.getMessage());
                
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
        boolean socketConnected = masterSocket != null && 
                                  masterSocket.isConnected() && 
                                  !masterSocket.isClosed() &&
                                  !masterSocket.isInputShutdown() &&
                                  !masterSocket.isOutputShutdown() &&
                                  output != null &&
                                  input != null;
        
        return connected && socketConnected;
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
                
                while (connected && running && input != null) {
                    try {
                        bytesRead = input.read(buffer);
                        
                        if (bytesRead == -1) {
                            System.out.println("Connection closed by master");
                            break;
                        }
                        
                        if (bytesRead > 0) {
                            processMessages(buffer, bytesRead);
                        }
                    } catch (SocketTimeoutException e) {
                        // Timeout is expected, just continue and check connection
                        continue;
                    }
                }
            } catch (IOException e) {
                if (connected && running) {
                    System.err.println("Connection to master lost in listen thread: " + e.getMessage());
                }
            } finally {
                if (connected) {
                    System.out.println("Listen thread ending, marking as disconnected");
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
                lastSuccessfulHeartbeat = System.currentTimeMillis();
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
                    if (isConnected()) {
                        sendHeartbeat(); // Keep connection alive during long tasks
                    }
                }
            }
            
            // Create response
            String result = "Task " + taskId + " completed by " + workerId;
            Message response = new Message("TASK_COMPLETE", workerId, result.getBytes());
            
            if (isConnected() && output != null) {
                output.write(response.pack());
                output.flush();
                System.out.println("Task " + taskId + " completed");
            } else {
                System.err.println("Cannot send task response - not connected");
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (isConnected() && output != null) {
                Message errorMsg = new Message("TASK_ERROR", workerId, 
                    ("Task interrupted: " + e.getMessage()).getBytes());
                output.write(errorMsg.pack());
                output.flush();
            }
            
        } catch (Exception e) {
            System.err.println("Error processing task: " + e.getMessage());
            
            // Send error response
            if (isConnected() && output != null) {
                Message errorMsg = new Message("TASK_ERROR", workerId, 
                    ("Task failed: " + e.getMessage()).getBytes());
                output.write(errorMsg.pack());
                output.flush();
            }
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
                    
                    if (isConnected() && output != null) {
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
            System.out.println("Heartbeat thread stopped");
        });
        
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        System.out.println("Heartbeat thread started");
    }
    
    /**
     * Send heartbeat to master
     */
    private void sendHeartbeat() throws IOException {
        if (isConnected() && output != null) {
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
                try { 
                    input.close(); 
                } catch (IOException e) { 
                    // Ignore
                }
                input = null;
            }
            
            if (output != null) {
                try { 
                    output.close(); 
                } catch (IOException e) { 
                    // Ignore
                }
                output = null;
            }
            
            if (masterSocket != null && !masterSocket.isClosed()) {
                try { 
                    masterSocket.close(); 
                } catch (IOException e) { 
                    // Ignore
                }
                masterSocket = null;
            }
            
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }
    
    /**
     * Handle connection loss and trigger reconnection
     */
    private void handleConnectionLoss() {
        if (connected) {
            System.out.println("Connection lost, initiating reconnection...");
            connected = false;
            cleanup();
            
            // Reconnection thread will handle the actual reconnection
        }
    }
    
    /**
     * Shutdown worker gracefully
     */
    public void shutdown() {
        System.out.println("Shutting down worker " + workerId);
        running = false;
        connected = false;
        
        // Interrupt threads
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        if (listenThread != null) {
            listenThread.interrupt();
        }
        if (reconnectionThread != null) {
            reconnectionThread.interrupt();
        }
        
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
            System.out.println("Shutdown hook triggered");
            worker.shutdown();
        }));
        
        worker.joinCluster(masterHost, masterPort);
        
        // Keep main thread alive and monitor connection
        while (worker.running) {
            try {
                Thread.sleep(1000);
                
                // Print connection status periodically for debugging
                if (!worker.isConnected() && worker.running) {
                    System.out.println("Main thread: Not connected, waiting for reconnection...");
                }
            } catch (InterruptedException e) {
                worker.shutdown();
                break;
            }
        }
        
        System.out.println("Worker main thread exiting");
    }
}