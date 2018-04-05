package org.apache.crail.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Created by atr on 04.04.18.
 */
public abstract class IOCtlCommand {
    public static final byte NOP       = 1;
    public static final byte NN_PING   = 2;
    public static final byte NN_DUMP   = 3;
    public static final byte DN_REMOVE = 4;

    public abstract int write(ByteBuffer buffer);
    public abstract void update(ByteBuffer buffer);
    public abstract int getSize();

    public static class RemoveDataNode extends IOCtlCommand {
        // 4 bytes for the IP address of the datanode
        public static int CSIZE = 4;
        private InetAddress address;

        RemoveDataNode(){
            this.address = null;
        }

        public RemoveDataNode(InetAddress address){
            this.address = address;
        }

        public InetAddress getIPAddress(){
            return this.address;
        }

        public int write(ByteBuffer buffer) {
            byte[] x = this.address.getAddress();
            buffer.put(x);
            return x.length;
        }

        public void update(ByteBuffer buffer) {
            byte[] barr = new byte[4];
            buffer.get(barr);
            try {
                this.address = InetAddress.getByAddress(barr);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        public int getSize(){
            return RemoveDataNode.CSIZE;
        }

        public String toString(){
            return "removeDN: " + (this.address == null ? " N/A" : this.address.toString());
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
