package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * 
 * For chunked messages:
 * [magic:8bytes][version:1byte][type:1byte][senderLen:2bytes][sender:variable]
 * [timestamp:8bytes][payloadLen:4bytes][chunkFlag:1byte][chunkId:4bytes][totalChunks:4bytes][payload:variable]
 */
public class Message {
    // Protocol constants
    public static final String MAGIC = "CSM218";  // Will be padded to 8 bytes
    public static final byte VERSION = 1;
    
    // Increased buffer sizes for large payloads
    public static final int MAX_BUFFER_SIZE = 1048576; // 1MB
    public static final int MAX_PAYLOAD_SIZE = 524288; // 512KB per chunk
    public static final int SOCKET_BUFFER_SIZE = 262144; // 256KB socket buffer
    
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
    public static final byte TYPE_CHUNK = 0x0A; // Chunk message type
    
    // Message fields - exactly matching specification
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;
    
    // Chunking fields
    private boolean isChunked = false;
    private int chunkId = 0;
    private int totalChunks = 1;
    private String originalType; // Store original type for chunked messages
    private static final byte CHUNKED_FLAG = 0x01;
    private static final byte MID_CHUNK_FLAG = 0x02;
    private static final byte FINAL_CHUNK_FLAG = 0x03;
    
    // Static chunk assembler for reassembling chunked messages
    private static final Map<String, ChunkAssembler> chunkAssemblers = new ConcurrentHashMap<>();
    
    /**
     * Inner class to handle chunk reassembly
     */
    private static class ChunkAssembler {
        private final String messageId;
        private final int totalChunks;
        private final byte[][] chunks;
        private int receivedChunks = 0;
        private long firstTimestamp;
        private String sender;
        private String originalType;
        
        public ChunkAssembler(String messageId, int totalChunks, String sender, String originalType, long timestamp) {
            this.messageId = messageId;
            this.totalChunks = totalChunks;
            this.chunks = new byte[totalChunks][];
            this.sender = sender;
            this.originalType = originalType;
            this.firstTimestamp = timestamp;
        }
        
        public synchronized boolean addChunk(int chunkId, byte[] data, long timestamp) {
            if (chunkId < 0 || chunkId >= totalChunks || chunks[chunkId] != null) {
                return false;
            }
            chunks[chunkId] = data;
            receivedChunks++;
            
            // Use earliest timestamp
            if (timestamp < firstTimestamp) {
                firstTimestamp = timestamp;
            }
            
            return receivedChunks == totalChunks;
        }
        
        public synchronized Message assemble() {
            if (receivedChunks != totalChunks) {
                return null;
            }
            
            // Calculate total size
            int totalSize = 0;
            for (byte[] chunk : chunks) {
                totalSize += chunk.length;
            }
            
            // Combine all chunks
            byte[] fullPayload = new byte[totalSize];
            int position = 0;
            for (byte[] chunk : chunks) {
                System.arraycopy(chunk, 0, fullPayload, position, chunk.length);
                position += chunk.length;
            }
            
            // Create reassembled message
            Message msg = new Message(originalType, sender, fullPayload);
            msg.timestamp = firstTimestamp;
            msg.magic = MAGIC;
            msg.version = VERSION;
            
            return msg;
        }
    }
    
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
     * Supports chunking for large payloads.
     */
    public byte[] pack() {
        try {
            // Validate required fields
            if (type == null || type.isEmpty()) {
                throw new IllegalArgumentException("Message type cannot be null or empty");
            }
            if (sender == null || sender.isEmpty()) {
                throw new IllegalArgumentException("Sender cannot be null or empty");
            }
            
            // Convert string type to byte
            byte typeByte = stringTypeToByte(this.type);
            
            // Convert sender to bytes
            byte[] senderBytes = this.sender.getBytes(StandardCharsets.UTF_8);
            if (senderBytes.length > 65535) {
                throw new IllegalArgumentException("Sender too long: " + senderBytes.length);
            }
            
            // Check if payload needs chunking
            if (this.payload != null && this.payload.length > MAX_PAYLOAD_SIZE) {
                return packAsChunk(typeByte, senderBytes, 0);
            }
            
            // Calculate total size
            int totalSize = 8 + 1 + 1 + 2 + senderBytes.length + 8 + 4;
            if (this.payload != null) {
                totalSize += this.payload.length;
            }
            
            // Validate size
            if (totalSize > MAX_BUFFER_SIZE) {
                throw new RuntimeException("Message too large: " + totalSize + " bytes. Consider chunking.");
            }
            
            // Create buffer
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            
            // Write magic (pad to 8 bytes)
            writeMagic(buffer);
            
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
     * Pack a message as a chunk (for large payloads)
     */
    private byte[] packAsChunk(byte typeByte, byte[] senderBytes, int chunkIndex) {
        int totalChunks = (int) Math.ceil((double) this.payload.length / MAX_PAYLOAD_SIZE);
        int start = chunkIndex * MAX_PAYLOAD_SIZE;
        int end = Math.min(start + MAX_PAYLOAD_SIZE, this.payload.length);
        byte[] chunkPayload = Arrays.copyOfRange(this.payload, start, end);
        
        // Generate unique message ID for reassembly
        String messageId = this.sender + "_" + this.timestamp + "_" + this.type;
        
        // Calculate size with chunking headers
        int totalSize = 8 + 1 + 1 + 2 + senderBytes.length + 8 + 4 + 1 + 4 + 4 + 4 + chunkPayload.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        // Write standard headers
        writeMagic(buffer);
        buffer.put((byte) this.version);
        buffer.put(TYPE_CHUNK); // Use chunk type
        buffer.putShort((short) senderBytes.length);
        buffer.put(senderBytes);
        buffer.putLong(this.timestamp);
        
        // Write payload length (includes chunking headers)
        buffer.putInt(1 + 4 + 4 + 4 + chunkPayload.length);
        
        // Write chunking info
        if (chunkIndex == 0) {
            buffer.put(CHUNKED_FLAG); // First chunk
        } else if (chunkIndex == totalChunks - 1) {
            buffer.put(FINAL_CHUNK_FLAG); // Last chunk
        } else {
            buffer.put(MID_CHUNK_FLAG); // Middle chunk
        }
        buffer.putInt(chunkIndex);
        buffer.putInt(totalChunks);
        
        // Write original type length and original type
        byte[] originalTypeBytes = this.type.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(originalTypeBytes.length);
        buffer.put(originalTypeBytes);
        
        // Write chunk payload
        buffer.put(chunkPayload);
        
        return buffer.array();
    }
    
    /**
     * Write magic bytes to buffer
     */
    private void writeMagic(ByteBuffer buffer) {
        byte[] magicBytes = new byte[8];
        byte[] magicSrc = this.magic.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(magicSrc, 0, magicBytes, 0, Math.min(magicSrc.length, 8));
        // Pad remaining bytes with spaces
        for (int i = magicSrc.length; i < 8; i++) {
            magicBytes[i] = ' ';
        }
        buffer.put(magicBytes);
    }
    
    /**
     * Reconstructs a Message from a byte stream.
     * Handles chunked messages automatically with reassembly.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            // Read magic
            byte[] magicBytes = new byte[8];
            buffer.get(magicBytes);
            String magic = new String(magicBytes, StandardCharsets.US_ASCII).trim();
            
            // Verify magic
            if (!MAGIC.equals(magic)) {
                throw new IllegalArgumentException("Invalid magic number: expected " + MAGIC + ", got " + magic);
            }
            
            // Read version
            int version = buffer.get();
            if (version != VERSION) {
                throw new IllegalArgumentException("Invalid version: expected " + VERSION + ", got " + version);
            }
            
            // Read message type
            byte typeByte = buffer.get();
            
            // Read sender
            short senderLen = buffer.getShort();
            byte[] senderBytes = new byte[senderLen];
            buffer.get(senderBytes);
            String sender = new String(senderBytes, StandardCharsets.UTF_8);
            
            // Read timestamp
            long timestamp = buffer.getLong();
            
            // Read payload length
            int payloadLen = buffer.getInt();
            
            // If this is a chunk, handle chunk reassembly
            if (typeByte == TYPE_CHUNK) {
                return handleChunk(buffer, magic, version, sender, timestamp, payloadLen);
            }
            
            // Regular message - read payload
            byte[] payload = null;
            if (payloadLen > 0) {
                payload = new byte[payloadLen];
                buffer.get(payload);
            }
            
            // Create and populate message
            Message msg = new Message();
            msg.magic = magic;
            msg.version = version;
            msg.type = byteToStringType(typeByte);
            msg.sender = sender;
            msg.timestamp = timestamp;
            msg.payload = payload;
            
            return msg;
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to unpack message: " + e.getMessage(), e);
        }
    }
    
    /**
     * Handle chunked message reassembly
     */
    private static Message handleChunk(ByteBuffer buffer, String magic, int version, 
                                      String sender, long timestamp, int totalPayloadLen) {
        // Read chunking info
        byte chunkFlag = buffer.get();
        int chunkId = buffer.getInt();
        int totalChunks = buffer.getInt();
        
        // Read original type
        int originalTypeLen = buffer.getInt();
        byte[] originalTypeBytes = new byte[originalTypeLen];
        buffer.get(originalTypeBytes);
        String originalType = new String(originalTypeBytes, StandardCharsets.UTF_8);
        
        // Read chunk payload
        int chunkPayloadLen = totalPayloadLen - 13; // Subtract chunk headers (1+4+4+4)
        byte[] chunkPayload = new byte[chunkPayloadLen];
        buffer.get(chunkPayload);
        
        // Generate unique message ID for this chunked message
        String messageId = sender + "_" + timestamp + "_" + originalType;
        
        // Get or create chunk assembler
        ChunkAssembler assembler = chunkAssemblers.computeIfAbsent(messageId, 
            k -> new ChunkAssembler(messageId, totalChunks, sender, originalType, timestamp));
        
        // Add chunk
        boolean isComplete = assembler.addChunk(chunkId, chunkPayload, timestamp);
        
        // If message is complete, assemble and return it
        if (isComplete) {
            Message assembledMsg = assembler.assemble();
            chunkAssemblers.remove(messageId);
            return assembledMsg;
        }
        
        // Otherwise return a partial message for acknowledgment
        Message partialMsg = new Message();
        partialMsg.magic = magic;
        partialMsg.version = version;
        partialMsg.type = "CHUNK_ACK";
        partialMsg.sender = sender;
        partialMsg.timestamp = timestamp;
        partialMsg.payload = ("Chunk " + chunkId + "/" + totalChunks + " received").getBytes(StandardCharsets.UTF_8);
        partialMsg.isChunked = true;
        partialMsg.chunkId = chunkId;
        partialMsg.totalChunks = totalChunks;
        
        return partialMsg;
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
            case "CHUNK": return TYPE_CHUNK;
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
            case TYPE_CHUNK: return "CHUNK";
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
     * Helper to create a task request with matrix data (with chunking support)
     */
    public static List<Message> createTaskRequestChunked(String workerId, String taskId, int[][] matrix) {
        List<Message> chunks = new ArrayList<>();
        
        // Convert matrix to byte array
        StringBuilder sb = new StringBuilder();
        sb.append(taskId).append(":");
        for (int[] row : matrix) {
            for (int val : row) {
                sb.append(val).append(",");
            }
            sb.append(";");
        }
        byte[] fullPayload = sb.toString().getBytes(StandardCharsets.UTF_8);
        
        // If payload is small, return single message
        if (fullPayload.length <= MAX_PAYLOAD_SIZE) {
            chunks.add(new Message("RPC_REQUEST", workerId, fullPayload));
            return chunks;
        }
        
        // Otherwise, create chunked messages
        Message baseMsg = new Message("RPC_REQUEST", workerId, fullPayload);
        int totalChunks = (int) Math.ceil((double) fullPayload.length / MAX_PAYLOAD_SIZE);
        
        for (int i = 0; i < totalChunks; i++) {
            int start = i * MAX_PAYLOAD_SIZE;
            int end = Math.min(start + MAX_PAYLOAD_SIZE, fullPayload.length);
            byte[] chunkPayload = Arrays.copyOfRange(fullPayload, start, end);
            
            Message chunkMsg = new Message("RPC_REQUEST", workerId, chunkPayload);
            chunkMsg.isChunked = true;
            chunkMsg.chunkId = i;
            chunkMsg.totalChunks = totalChunks;
            chunkMsg.originalType = "RPC_REQUEST";
            chunks.add(chunkMsg);
        }
        
        return chunks;
    }
    
    /**
     * Helper to create a task request
     */
    public static Message createTaskRequest(String workerId, String taskId, int[][] matrix) {
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
    
    /**
     * Check if this message is a chunk
     */
    public boolean isChunk() {
        return isChunked;
    }
    
    /**
     * Get chunk ID
     */
    public int getChunkId() {
        return chunkId;
    }
    
    /**
     * Get total chunks
     */
    public int getTotalChunks() {
        return totalChunks;
    }
    
    /**
     * Get original type for chunked messages
     */
    public String getOriginalType() {
        return originalType;
    }
    
    /**
     * Validate message against protocol specification
     */
    public void validate() {
        if (!MAGIC.equals(magic)) {
            throw new IllegalStateException("Invalid magic: " + magic);
        }
        if (version != VERSION) {
            throw new IllegalStateException("Invalid version: " + version);
        }
        if (type == null || type.isEmpty()) {
            throw new IllegalStateException("Type cannot be null or empty");
        }
        if (sender == null || sender.isEmpty()) {
            throw new IllegalStateException("Sender cannot be null or empty");
        }
        if (timestamp <= 0) {
            throw new IllegalStateException("Invalid timestamp: " + timestamp);
        }
    }
    
    @Override
    public String toString() {
        return "Message{type='" + type + "', sender='" + sender + "', timestamp=" + timestamp + 
               ", payloadSize=" + (payload != null ? payload.length : 0) + 
               (isChunked ? ", chunk=" + chunkId + "/" + totalChunks : "") + "}";
    }
}