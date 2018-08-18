package org.apache.hadoop.hdfs.plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

class CompressionFSDataOutputStream extends GZIPOutputStream {

    /**
     * Creates an output stream filter built on top of the specified
     * underlying output stream.
     *
     * @param out the underlying output stream to be assigned to
     *            the field <tt>this.out</tt> for later use, or
     *            <code>null</code> if this instance is to be
     *            created without an underlying stream.
     */
    CompressionFSDataOutputStream(OutputStream out) throws IOException {
        super(out);
    }
}

