public class Message {
    // MUST have these exact fields
    public String magic;        // Must be "CSM218"
    public int version;          // Must be 1
    public String type;          // Must match required types
    public String sender;         // Worker ID or "master"
    public long timestamp;        // System.currentTimeMillis()
    public byte[] payload;        // Raw data
    
    // Constructor must initialize these
    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Use ONLY DataOutputStream/DataInputStream - no ByteBuffer?
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write exactly in this order
        dos.writeUTF(magic);           // Use writeUTF for strings
        dos.writeInt(version);
        dos.writeUTF(type);
        dos.writeUTF(sender);
        dos.writeLong(timestamp);
        dos.writeInt(payload.length);
        dos.write(payload);
        
        return baos.toByteArray();
    }
    
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        Message msg = new Message();
        msg.magic = dis.readUTF();
        msg.version = dis.readInt();
        msg.type = dis.readUTF();
        msg.sender = dis.readUTF();
        msg.timestamp = dis.readLong();
        int payloadLen = dis.readInt();
        msg.payload = new byte[payloadLen];
        dis.readFully(msg.payload);
        
        return msg;
    }
}