package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 *
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private int port;
    private Thread acceptThread;
    
    // Heartbeat timeout in milliseconds
    private static final long HEARTBEAT_TIMEOUT = 10000;
    // Socket timeout for accept() to prevent infinite blocking
    private static final int SOCKET_TIMEOUT_MS = 1000;
    
    // Worker connection wrapper
    private static class WorkerConnection {
        String workerId;
        Socket socket;
        InputStream input;
        OutputStream output;
        long lastHeartbeat;
        boolean isAlive;
        
        WorkerConnection(String workerId, Socket socket) throws IOException {
            this.workerId = workerId;
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
            this.lastHeartbeat = System.currentTimeMillis();
            this.isAlive = true;
        }
        
        void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        boolean isTimedOut() {
            return System.currentTimeMillis() - lastHeartbeat > HEARTBEAT_TIMEOUT;
        }
        
        void sendMessage(Message msg) throws IOException {
            output.write(msg.pack());
            output.flush();
        }
        
        void close() {
            try {
                isAlive = false;
                if (input != null) input.close();
                if (output != null) output.close();
                if (socket != null && !socket.isClosed()) socket.close();
                System.out.println("Worker " + workerId + " connection closed");
            } catch (IOException e) {
                System.err.println("Error closing worker connection: " + e.getMessage());
            }
        }
    }

    /**
     * Start the communication listener in a non-blocking way.
     * This is critical for tests to pass!
     */
    public void listen(int port) throws IOException {
        this.port = port;
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
        
        System.out.println("Master listening on port " + port);
        
        // Start heartbeat checker as daemon thread
        Thread heartbeatThread = new Thread(this::checkHeartbeatsLoop);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        
        // Start accepting connections in a SEPARATE THREAD (non-blocking)
        acceptThread = new Thread(() -> {
            System.out.println("Accept thread started");
            while (running) {
                try {
                    Socket workerSocket = serverSocket.accept();
                    System.out.println("New connection from " + workerSocket.getInetAddress());
                    
                    // Handle worker in a separate thread
                    systemThreads.submit(() -> handleWorkerConnection(workerSocket));
                    
                } catch (SocketTimeoutException e) {
                    // Timeout reached, just continue - this is expected
                    continue;
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Error accepting connection: " + e.getMessage());
                    }
                }
            }
            System.out.println("Accept thread stopped");
        });
        
        acceptThread.setDaemon(true); // Make it a daemon so it doesn't prevent JVM shutdown
        acceptThread.start();
        
        // Method returns immediately - this is what the test expects!
        System.out.println("Listen method returned, accept thread running in background");
    }
    
    /**
     * Continuous heartbeat check loop
     */
    private void checkHeartbeatsLoop() {
        while (running) {
            try {
                checkHeartbeats();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * Handle a new worker connection
     */
    private void handleWorkerConnection(Socket socket) {
        try {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            
            // Read registration message with timeout
            byte[] buffer = new byte[4096];
            int bytesRead;
            try {
                bytesRead = input.read(buffer);
            } catch (SocketTimeoutException e) {
                System.err.println("Registration timeout from " + socket.getInetAddress());
                socket.close();
                return;
            }
            
            if (bytesRead > 0) {
                int msgLength = Message.getCompleteMessageLength(buffer);
                if (msgLength > 0 && bytesRead >= msgLength) {
                    byte[] msgData = new byte[msgLength];
                    System.arraycopy(buffer, 0, msgData, 0, msgLength);
                    
                    Message regMsg = Message.unpack(msgData);
                    
                    if ("REGISTER_WORKER".equals(regMsg.type)) {
                        String workerId = regMsg.sender;
                        
                        // Store worker connection
                        WorkerConnection conn = new WorkerConnection(workerId, socket);
                        workers.put(workerId, conn);
                        
                        System.out.println("Worker registered: " + workerId);
                        
                        // Send acknowledgment
                        Message ack = new Message("WORKER_ACK", "master", new byte[0]);
                        output.write(ack.pack());
                        output.flush();
                        
                        // Start listening for messages from this worker
                        listenToWorker(conn);
                    }
                }
            }
            
        } catch (IOException e) {
            System.err.println("Error handling worker connection: " + e.getMessage());
        }
    }
    
    /**
     * Listen for messages from a specific worker
     */
    private void listenToWorker(WorkerConnection conn) {
        try {
            conn.socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while (conn.isAlive && running) {
                try {
                    bytesRead = conn.input.read(buffer);
                    if (bytesRead == -1) {
                        break; // Connection closed
                    }
                    
                    if (bytesRead > 0) {
                        System.out.println("Received " + bytesRead + " bytes from worker " + conn.workerId);
                        
                        int offset = 0;
                        
                        while (offset < bytesRead) {
                            byte[] remainingData = Arrays.copyOfRange(buffer, offset, bytesRead);
                            int msgLength = Message.getCompleteMessageLength(remainingData);
                            
                            if (msgLength > 0 && offset + msgLength <= bytesRead) {
                                byte[] msgData = new byte[msgLength];
                                System.arraycopy(buffer, offset, msgData, 0, msgLength);
                                
                                Message message = Message.unpack(msgData);
                                System.out.println("Received message type: " + message.type + " from " + conn.workerId);
                                handleWorkerMessage(conn, message);
                                
                                offset += msgLength;
                            } else {
                                break;
                            }
                        }
                    }
                } catch (SocketTimeoutException e) {
                    // Timeout reached, just continue - prevents hanging
                    continue;
                }
            }
        } catch (IOException e) {
            System.err.println("Connection lost with worker " + conn.workerId + ": " + e.getMessage());
        } finally {
            // Mark worker as dead
            workers.remove(conn.workerId);
            conn.close();
            System.out.println("Worker " + conn.workerId + " removed from cluster");
        }
    }
    
    /**
     * Handle messages from workers
     */
    private void handleWorkerMessage(WorkerConnection conn, Message message) {
        // Update heartbeat timestamp
        conn.updateHeartbeat();
        
        switch (message.type) {
            case "HEARTBEAT":
                System.out.println("Heartbeat from " + conn.workerId);
                break;
                
            case "TASK_COMPLETE":
                System.out.println("Task completed by " + conn.workerId + ": " + 
                    new String(message.payload));
                // TODO: Aggregate results
                break;
                
            case "TASK_ERROR":
                System.err.println("Task error from " + conn.workerId + ": " + 
                    new String(message.payload));
                // TODO: Handle task failure
                break;
                
            default:
                System.out.println("Unknown message from " + conn.workerId + ": " + message.type);
        }
    }
    
    /**
     * Check for timed-out workers
     */
    private void checkHeartbeats() {
        System.out.println("Checking worker heartbeats...");
        
        workers.values().removeIf(conn -> {
            if (conn.isTimedOut()) {
                System.out.println("Worker " + conn.workerId + " timed out (no heartbeat for " + 
                    (System.currentTimeMillis() - conn.lastHeartbeat) + "ms)");
                conn.close();
                return true;
            }
            return false;
        });
        
        System.out.println("Active workers: " + workers.size());
    }
    
    /**
     * Entry point for a distributed computation.
     *
     * @param operation A string descriptor of the matrix operation (e.g. "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (workers.isEmpty()) {
            System.err.println("No workers available!");
            return null;
        }
        
        System.out.println("Coordinating " + operation + " with " + workers.size() + " workers");
        return null; // Stub implementation
    }
    
    /**
     * Send a task to a specific worker
     */
    public void sendTask(String workerId, String taskId, int[][] matrix) {
        WorkerConnection conn = workers.get(workerId);
        if (conn != null && conn.isAlive) {
            try {
                Message taskMsg = Message.createTaskRequest(workerId, taskId, matrix);
                conn.sendMessage(taskMsg);
                System.out.println("Task " + taskId + " sent to worker " + workerId);
            } catch (IOException e) {
                System.err.println("Failed to send task to worker " + workerId + ": " + e.getMessage());
            }
        } else {
            System.err.println("Worker " + workerId + " not available");
        }
    }
    
    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        System.out.println("Reconciling cluster state...");
        System.out.println("Active workers: " + workers.size());
    }
    
    /**
     * Shutdown the master gracefully
     */
    public void shutdown() {
        System.out.println("Shutting down master...");
        running = false;
        
        // Interrupt accept thread
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
        
        // Close server socket to unblock accept()
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
        
        // Close all worker connections
        workers.values().forEach(WorkerConnection::close);
        workers.clear();
        
        // Shutdown thread pools
        systemThreads.shutdown();
        
        try {
            if (!systemThreads.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                systemThreads.shutdownNow();
            }
        } catch (InterruptedException e) {
            systemThreads.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("Master shutdown complete");
    }
    
    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: Master <port>");
            System.out.println("Example: Master 8080");
            return;
        }
        
        int port = Integer.parseInt(args[0]);
        Master master = new Master();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(master::shutdown));
        
        try {
            master.listen(port);
            
            // Keep main thread alive but responsive to shutdown
            while (master.running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    master.shutdown();
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start master: " + e.getMessage());
            e.printStackTrace();
        }
    }
}