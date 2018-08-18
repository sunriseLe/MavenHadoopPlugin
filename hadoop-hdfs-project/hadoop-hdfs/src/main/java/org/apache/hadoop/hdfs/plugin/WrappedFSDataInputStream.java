package org.apache.hadoop.hdfs.plugin;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class WrappedFSDataInputStream
        extends FilterInputStream
        implements Seekable, PositionedReadable, ByteBufferReadable {

    /**
     * Creates a <code>FilterInputStream</code>
     * by assigning the  argument <code>in</code>
     * to the field <code>this.in</code> so as
     * to remember it for later use.
     *
     * @param in the underlying input stream, or <code>null</code> if
     *           this instance is to be created without an underlying stream.
     */
    public WrappedFSDataInputStream(DFSInputStream in) throws IOException {
        super(in);
        // dfsClient can be used to access all APIs, such as getXAttr
        DFSClient dfsClient = in.getDfsClient();
        // src is the path of input file or directory
        String src = in.getSrc();
        // Add plugins below
        // Compress plugin
        if (dfsClient.getXAttr(src, "user.compress") != null) {
            this.in = new CompressionFSDataInputStream(this.in);
        }
        // Dynamic replication plugin
        DynamicReplicationPlugin.allocateReplication(src, dfsClient);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return ((PositionedReadable) in).read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        ((PositionedReadable) in).readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        ((PositionedReadable) in).readFully(position, buffer);
    }

    @Override
    public void seek(long pos) throws IOException {
        ((Seekable) in).seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return ((Seekable) in).getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return ((Seekable) in).seekToNewSource(targetPos);
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        return ((ByteBufferReadable) in).read(buf);
    }
}
