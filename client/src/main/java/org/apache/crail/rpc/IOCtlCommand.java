package org.apache.crail.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Created by atr on 04.04.18.
 */
public abstract class IOCtlCommand {
    public static final byte NOP       = 1;
    public static final byte DN_REMOVE = 2;

    public abstract int write(ByteBuffer buffer) throws IOException;
    public abstract void update(ByteBuffer buffer) throws IOException;
    public abstract int getSize();

    public static class RemoveDataNode extends IOCtlCommand {
        // 4 byte IP + 4 byte port (java short are signed hence, we need 4 byte numbers
        public static int CSIZE = 8;
        private InetAddress address;
        private int port;

        RemoveDataNode(){
            this.address = null;
            this.port = -1;
        }

        public RemoveDataNode(InetAddress address, int port){
            this.address = address;
            this.port = port;
        }

        public InetAddress getIPAddress(){
            return this.address;
        }

        public int port(){
            return this.port;
        }

        public int write(ByteBuffer buffer) throws IOException {
            byte[] x = this.address.getAddress();
            if(RemoveDataNode.CSIZE > buffer.remaining()) {
                throw new IOException("Write ByteBuffer is too small, remaining " + buffer.remaining() + " expected, " + RemoveDataNode.CSIZE + " bytes");
            }
            buffer.put(x);
            buffer.putInt(this.port);
            return RemoveDataNode.CSIZE;
        }

        public void update(ByteBuffer buffer) throws IOException {
            byte[] barr = new byte[4]; // 4 bytes for the IP address
            if(getSize() > buffer.remaining()) {
                throw new IOException("Read ByteBuffer is too small, remaining " + buffer.remaining() + " expected, " + getSize() + " bytes");
            }
            buffer.get(barr);
            try {
                this.address = InetAddress.getByAddress(barr);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            this.port = buffer.getInt();
        }

        public int getSize(){
            return RemoveDataNode.CSIZE;
        }

        public String toString(){
            return "removeDN: " + (this.address == null ? " N/A" : (this.address.toString() + "/port: " + this.port));
        }
    }

    public static class NoOpCommand extends IOCtlCommand {

        NoOpCommand(){}

        public int write(ByteBuffer buffer){return  0;}

        public void update(ByteBuffer buffer){}

        public int getSize(){ return 0;}

        public String toString(){ return "NoOpCommand";}
    }
}
