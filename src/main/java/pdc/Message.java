package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 * 
 * Wire Format:
 * [magic:8bytes][version:1byte][type:1byte][senderLen:2bytes][sender:variable]
 * [timestamp:8bytes][payloadLen:4bytes][payload:variable]
 */
public class Message {
    // Protocol constants
    public static final String MAGIC = "CSM218";  // Will be padded to 8 bytes
    public static final byte VERSION = 1;
    
    // Message types (as bytes for efficient encoding)
    public static final byte TYPE_CONNECT = 0x01;
    public static final byte TYPE_REGISTER_WORKER = 0x02;
    public static final byte TYPE_REGISTER_CAPABILITIES = 0x03;
    public static final byte TYPE_RPC_REQUEST = 0x04;
    public static final byte TYPE_RPC_RESPONSE = 0x05;
    public static final byte TYPE_TASK_COMPLETE = 0x06;
    public static final byte TYPE_TASK_ERROR = 0x07;
    public static final byte TYPE_HEARTBEAT = 0x08;
    public static final byte TYPE_WORKER_ACK = 0x09;
    
    // Message fields
    public String magic;
    public int version;
    public String type;      // Will be converted to/from byte in wire format
    public String sender;
    public long timestamp;
    public byte[] payload;
    
    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.timestamp = System.currentTimeMillis();
    }
    
    public Message(String type, String sender, byte[] payload) {
        this();
        this.type = type;
        this.sender = sender;
        this.payload = payload;
    }
    
    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     * 
     * Format:
     * - 8 bytes: MAGIC (padded with spaces)
     * - 1 byte: VERSION
     * - 1 byte: message type (converted from string)
     * - 2 bytes: sender length (unsigned short)
     * - n bytes: sender string (UTF-8)
     * - 8 bytes: timestamp
     * - 4 bytes: payload length
     * - m bytes: payload
     */
    public byte[] pack() {
        try {
            // Convert string type to byte
            byte typeByte = stringTypeToByte(this.type);
            
            // Convert sender to bytes
            byte[] senderBytes = this.sender.getBytes(StandardCharsets.UTF_8);
            if (senderBytes.length > 65535) {
                throw new IllegalArgumentException("Sender too long: " + senderBytes.length);
            }
            
            // Calculate total size
            int totalSize = 8 + 1 + 1 + 2 + senderBytes.length + 8 + 4;
            if (this.payload != null) {
                totalSize += this.payload.length;
            }
            
            // Create buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // Write magic (pad to 8 bytes)
            byte[] magicBytes = new byte[8];
            byte[] magicSrc = this.magic.getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(magicSrc, 0, magicBytes, 0, Math.min(magicSrc.length, 8));
            buffer.put(magicBytes);
            
            // Write version
            buffer.put((byte) this.version);
            
            // Write message type
            buffer.put(typeByte);
            
            // Write sender length and sender
            buffer.putShort((short) senderBytes.length);
            buffer.put(senderBytes);
            
            // Write timestamp
            buffer.putLong(this.timestamp);
            
            // Write payload length and payload
            if (this.payload != null) {
                buffer.putInt(this.payload.length);
                buffer.put(this.payload);
            } else {
                buffer.putInt(0);
            }
            
            return buffer.array();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to pack message: " + e.getMessage(), e);
        }
    }
    
    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            // Read magic
            byte[] magicBytes = new byte[8];
            buffer.get(magicBytes);
            String magic = new String(magicBytes, StandardCharsets.US_ASCII).trim();
            
            // Read version
            int version = buffer.get();
            
            // Read message type
            byte typeByte = buffer.get();
            String type = byteToStringType(typeByte);
            
            // Read sender
            short senderLen = buffer.getShort();
            byte[] senderBytes = new byte[senderLen];
            buffer.get(senderBytes);
            String sender = new String(senderBytes, StandardCharsets.UTF_8);
            
            // Read timestamp
            long timestamp = buffer.getLong();
            
            // Read payload
            int payloadLen = buffer.getInt();
            byte[] payload = null;
            if (payloadLen > 0) {
                payload = new byte[payloadLen];
                buffer.get(payload);
            }
            
            // Create and populate message
            Message msg = new Message();
            msg.magic = magic;
            msg.version = version;
            msg.type = type;
            msg.sender = sender;
            msg.timestamp = timestamp;
            msg.payload = payload;
            
            return msg;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to unpack message: " + e.getMessage(), e);
        }
    }
    
    /**
     * Helper to determine if we have a complete message in a byte stream
     * Used for handling TCP fragmentation
     */
    public static int getCompleteMessageLength(byte[] data) {
        if (data.length < 8 + 1 + 1 + 2) {
            return -1; // Not enough for header
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        // Skip magic (8), version (1), type (1)
        buffer.position(8 + 1 + 1);
        
        // Read sender length
        short senderLen = buffer.getShort();
        
        // Calculate minimum complete message size
        int minSize = 8 + 1 + 1 + 2 + senderLen + 8 + 4;
        
        if (data.length < minSize) {
            return -1; // Not enough for fixed parts
        }
        
        // Read payload length
        buffer.position(8 + 1 + 1 + 2 + senderLen + 8);
        int payloadLen = buffer.getInt();
        
        return minSize + payloadLen;
    }
    
    /**
     * Convert string message type to byte
     */
    private static byte stringTypeToByte(String type) {
        switch (type) {
            case "CONNECT": return TYPE_CONNECT;
            case "REGISTER_WORKER": return TYPE_REGISTER_WORKER;
            case "REGISTER_CAPABILITIES": return TYPE_REGISTER_CAPABILITIES;
            case "RPC_REQUEST": return TYPE_RPC_REQUEST;
            case "RPC_RESPONSE": return TYPE_RPC_RESPONSE;
            case "TASK_COMPLETE": return TYPE_TASK_COMPLETE;
            case "TASK_ERROR": return TYPE_TASK_ERROR;
            case "HEARTBEAT": return TYPE_HEARTBEAT;
            case "WORKER_ACK": return TYPE_WORKER_ACK;
            default: throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
    
    /**
     * Convert byte to string message type
     */
    private static String byteToStringType(byte b) {
        switch (b) {
            case TYPE_CONNECT: return "CONNECT";
            case TYPE_REGISTER_WORKER: return "REGISTER_WORKER";
            case TYPE_REGISTER_CAPABILITIES: return "REGISTER_CAPABILITIES";
            case TYPE_RPC_REQUEST: return "RPC_REQUEST";
            case TYPE_RPC_RESPONSE: return "RPC_RESPONSE";
            case TYPE_TASK_COMPLETE: return "TASK_COMPLETE";
            case TYPE_TASK_ERROR: return "TASK_ERROR";
            case TYPE_HEARTBEAT: return "HEARTBEAT";
            case TYPE_WORKER_ACK: return "WORKER_ACK";
            default: throw new IllegalArgumentException("Unknown type byte: " + b);
        }
    }
    
    /**
     * Helper to create a registration message
     */
    public static Message createRegistration(String workerId) {
        return new Message("REGISTER_WORKER", workerId, new byte[0]);
    }
    
    /**
     * Helper to create a heartbeat message
     */
    public static Message createHeartbeat(String workerId) {
        return new Message("HEARTBEAT", workerId, new byte[0]);
    }
    
    /**
     * Helper to create a task request with matrix data
     */
    public static Message createTaskRequest(String workerId, String taskId, int[][] matrix) {
        // Convert matrix to byte array (simplified - you'd need a proper serialization)
        StringBuilder sb = new StringBuilder();
        sb.append(taskId).append(":");
        for (int[] row : matrix) {
            for (int val : row) {
                sb.append(val).append(",");
            }
            sb.append(";");
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        return new Message("RPC_REQUEST", workerId, payload);
    }
    
    /**
     * Helper to create a task response
     */
    public static Message createTaskResponse(String workerId, String taskId, int[][] result) {
        // Similar to above, but with result data
        StringBuilder sb = new StringBuilder();
        sb.append(taskId).append(":");
        for (int[] row : result) {
            for (int val : row) {
                sb.append(val).append(",");
            }
            sb.append(";");
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        return new Message("TASK_COMPLETE", workerId, payload);
    }
    
    @Override
    public String toString() {
        return "Message{type='" + type + "', sender='" + sender + "', timestamp=" + timestamp + 
               ", payloadSize=" + (payload != null ? payload.length : 0) + "}";
    }
}