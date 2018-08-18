package org.apache.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.Progressable;

public class DFSClientTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			DFSClient client = new DFSClient(new URI("hdfs://192.168.202.34"),
					new Configuration());
		    System.out.println("集群默认文件块大小："+client.getDefaultBlockSize()/1024/1024+"MB");
			System.out.println("集群默认副本数为："+client.getDefaultReplication());
			
			/*UploadFile(client, "/home/lucy/下载/WebStorm-2018.1.5.tar.gz", 
					"/test/gzip/WebStorm-2018.1.5.tar.gz");*/
			/*UploadFile(client, "/home/lucy/文档/InterfaceAudience.java", 
					"/test/gzip/InterfaceAudience.java");
			DownloadFile(client, "/test/gzip/InterfaceAudience.java",
					"/home/lucy/InterfaceAudience.java");*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*测试getBlockStorageLocations*/
	public static void TestGetBlockStorageLocations(DFSClient client,String src,
			long start,long lenght) {
		try {
			/*获取BlockLocation[]并将其构造为List<BlockLocation> */
			BlockLocation[] locations = client.getBlockLocations(src,start, lenght);
			List<BlockLocation> blocks=new ArrayList<>();
			for(int i=0;i<locations.length;i++){
				blocks.add(locations[i]);
			}
			/*getBlockStorageLocation由于client conf问题，不支持
			 * 报错信息：Datanode-side support for getVolumeBlockLocations() must also 
			 * be enabled in the client configuration.
			 */
			BlockStorageLocation[] storageLocations=client.getBlockStorageLocations(blocks);
			
			/*BlockStorageLocation其实是在BlockLocation的基础上多了VolumeId[]*/
			for(int j=0;j<storageLocations.length;j++){
				VolumeId[] ids=storageLocations[j].getVolumeIds();
				for(int m=0;m<ids.length;m++){
					System.out.println(ids[m].hashCode());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*测试getBlockLocations*/
	public static void TestGetBlockLocations(DFSClient client,String src,
			long start,long length) {
		/*获取数据块存储的文件offset、这个块中存储的文件长度、块所在的dataNode的hostName
		 * 
		 *默认数据块大小为128MB，一个文件大小为225MB，则获取文件offset为150*1024*1024所在的数据块位置时，
		 *返回的是“134217728,109510292,osdserver1,osdserver2,monserver”
		 *
		 *其中134217728=128MB，表示这个数据块存储的是文件从128MB开始的内容
		 *
		 *109510292=104MB，表示这个数据块存储了的文件大小为104MB
		 *没有达到默认块大小，说明文件在这个数据块中的存储已经结束
		 *
		 *综合说明整个文件的大小为232MB，这与通过以下代码获取的文件大小一致
		 *HdfsFileStatus fileInfo=client.getFileInfo("/test/gzip/WebStorm-2018.1.5.tar.gz");
		 *System.out.println("/test/gzip/WebStorm-2018.1.5.tar.gz大小："+fileInfo.getLen()/1024/1024+"MB");
		 */
		try {
			BlockLocation[] locations = client.getBlockLocations(src,
					start, length);
			for( int i=0;i<locations.length;i++){
				System.out.println(locations[i].toString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	/*文件上传,使用压缩后的HdfsDataOutputStream*/
	public static void UploadFile(DFSClient client,String src,String dst) {
		int byteLen=0;
	    byte[] buffer=new byte[1024];
	    try {
	    	/*先创建文件*/
	    	OutputStream out= client.create(dst,true);
	    	client.setPermission(dst, new FsPermission(
	    			FsAction.ALL,FsAction.ALL,FsAction.ALL));
	    	out.close();
	    	
	    	/*通过append()创建压缩后的输出流*/
	    	HdfsDataOutputStream dataOutputStream=client.append(dst, 1024, EnumSet.of(CreateFlag.APPEND), 
	    			new Progressable() {
						@Override
						public void progress() {
							// TODO Auto-generated method stub
						}
					}, null);
	    	BufferedInputStream in = new BufferedInputStream(new FileInputStream(src));
	    	
	    	/*文件上传，写入到HDFS*/
	    	while((byteLen=in.read(buffer))!=-1){
	    		dataOutputStream.write(buffer, 0, byteLen);//指定每次写入的数据长度为实际读取的数据长度
	    		System.out.println(byteLen);
	    	}
	    	
	    	/*关闭输入、输出流*/
			dataOutputStream.flush();
			dataOutputStream.close();
			in.close();
			
			System.out.println("文件上传完成！");
			}catch (IOException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
      }
	
	/*文件下载*/
	public static void DownloadFile(DFSClient client,String src,String dst) {
		int byteLen=0;
		byte[] buffer=new byte[1024];
		try{
			/*创建解压缩后的输入流*/
			DFSInputStream in=client.open(src);
			HdfsDataInputStream dataInputStream=client.createWrappedInputStream(in);
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dst));
			
			/*文件下载，写入本地磁盘*/
			while ((byteLen = dataInputStream.read(buffer)) !=-1)
			{
				System.out.println(byteLen);
				out.write(buffer,0,byteLen);//指定每次写入的数据长度为实际读取的数据长度
			}
			
			/*关闭输入、输出流*/
			out.flush();
			out.close();
			dataInputStream.close();
			System.out.println("文件已下载！");
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}      
		}

}
