package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class WrappedFSDataInputStream
        extends GZIPInputStream
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
    WrappedFSDataInputStream(DFSInputStream in) throws IOException {
        super(in);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    	//开始seek
    	//long totalLen=0;
    	long remainLen=position;
    	byte[] buf=new byte[2048];
    	int readLen=0;
    	int startLen=0;
    	int len=0;
    	int relLen=0;
    	
    	while(remainLen>0){
    		readLen=read(buf,0,2048);
    		if (readLen!=0) {
    			//totalLen+=readLen;
        		remainLen=remainLen-readLen;
			}else{
				break;
			}
    	}
    	
    	if (remainLen< 0) {
			startLen=readLen+(int)remainLen;
			len=(int)(-remainLen);
			System.arraycopy(buf, startLen, buffer, offset, len);
			length=length-len;
			offset=offset+len;
		}else if (remainLen>0) {
			throw new IOException();
		}
    	
    	//seek完毕
    	relLen=len+read(buffer, offset, length);
    	return relLen;
    	
        //return ((PositionedReadable) in).read(position, buffer, offset, length);
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
    	/*long totalLen=0;
    	long remainLen=pos;
    	byte[] buf=new byte[2048];
    	int readLen=0;
    	
    	while(remainLen>0){
    		readLen=read(buf,0,2048);
    		if (readLen!=0) {
    			totalLen+=readLen;
        		remainLen=remainLen-readLen;
			}else{
				break;
			}
    	}
    	
    	if (remainLen< 0) {
			int lastLen=readLen+(int)remainLen;
		}else if (remainLen>0) {
			throw new IOException();
		}*/
    	
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
    
    @Override
    public int read(byte[] buf, int off, int len) throws IOException{
    	int relLen=0;
    	int remainLen=len;
    	int tempLen=0;
    	
    	while(relLen<len){
    		tempLen=super.read(buf, off, remainLen);
    		off+=tempLen;
    		if (tempLen!=-1) {
				relLen+=tempLen;
				remainLen=remainLen-tempLen;
				//System.out.println(relLen+","+remainLen);
			}else {
				break;
			}	
    	}
    	
    	return relLen;
    }
    
}
