package org.apache.hadoop.hdfs.plugin;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;

import java.io.FilterOutputStream;
import java.io.IOException;

public class WrappedFSDataOutputStream extends FilterOutputStream {

    /**
     * Creates an output stream filter built on top of the specified
     * underlying output stream.
     *
     * @param out the underlying output stream to be assigned to
     *            the field <tt>this.out</tt> for later use, or
     *            <code>null</code> if this instance is to be
     *            created without an underlying stream.
     */
    public WrappedFSDataOutputStream(DFSOutputStream out) throws IOException {
        super(out);
        // dfsClient can be used to access all APIs, such as getXAttr
        DFSClient dfsClient = out.getDfsClient();
        // src is the path of input file or directory
        String src = out.getSrc();
        // Add plugins below
        if (dfsClient.getXAttr(src, "user.compress") != null) {
            this.out = new CompressionFSDataOutputStream(this.out);
        }
    }
}
