package org.apache.crail;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by atr on 11.04.18.
 */
abstract public class IOCtlResponse {
    public abstract int write(ByteBuffer buffer) throws IOException;
    public abstract void update(ByteBuffer buffer) throws IOException;
    public abstract int getSize();

    public static class IOCtlEmptyResp extends IOCtlResponse {
        public static int CSIZE = 0;

        public IOCtlEmptyResp(){
        }

        public int write(ByteBuffer buffer) throws IOException {
            return IOCtlEmptyResp.CSIZE;
        }

        public void update(ByteBuffer buffer) throws IOException {
        }

        public int getSize(){
            return IOCtlEmptyResp.CSIZE;
        }

        public String toString(){
            return "IOCtlResponse: Empty";
        }
    }
}
