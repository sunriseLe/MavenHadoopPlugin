package org.apache.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;


public class DFileSystemTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			/*创建DistributedFileSystem实例并初始化*/
			System.setProperty("HADOOP_USER_NAME", "cephlee");
			DistributedFileSystem dfs=new DistributedFileSystem();
			dfs.initialize(new URI("hdfs://192.168.202.34"),new Configuration());
			
			
			//UploadFile(dfs, "/home/lucy/文档/1.txt", "1.txt");
			//DownloadFile(dfs, "/test/mkddir/1.txt","/home/lucy/1.txt" );
			
			
			/*UploadFile(dfs, "/home/lucy/文档/1.txt", "/test/1.txt");*/
			//AppendFile(dfs, "/home/lucy/文档/1.txt", "/test/1.txt");
			
			/*byte[] value=("false").getBytes("utf-8");
			dfs.setXAttr(new Path("/"), "user.zip", value);*/
			
			//UploadFile(dfs, "/home/lucy/文档/1.txt", "/test/mkdir/1.txt");
			
			/*byte[] value=("true").getBytes("utf-8");
			dfs.setXAttr(new Path("/"), "user.zip", value);
			
			UploadFile(dfs, "/home/lucy/文档/1.txt", "/1.txt");*/
			//UploadFile(dfs, "/home/lucy/文档/1.txt", "/1.txt");

			
			/*UploadFile(dfs, "/home/lucy/文档/big.txt",
					"/test/big.txt");*/
			
			/*boolean result=getParentDir(dfs, "1.txt");
			System.out.println(result);*/

			/*boolean result=dfs.getParentDir("/user/lucy/1.txt");
			System.out.println(result);*/
			
			/*byte[] value=("true").getBytes("utf-8");
			dfs.setXAttr(new Path("/"), "user.zip", value);
			dfs.removeXAttr(new Path("/"), "user.zip");*/
			
			
			/*dfs.delete(new Path("/"),true);*/
			
			} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static boolean getParentDir(DistributedFileSystem dfs,String path) {
		File file=new File(path);
		//父目录不存在，只有一种情况：path为1.txt的形式，这时存储在/user/HADOOP_USER_NAME的目录下
		if (file.getParent()==null) {
			//获取HADOOP_USER_NAME来判断父目录的xattr
			//通过System.getProperty获取HADOOP_USER_NAME
			String user=System.getProperty("HADOOP_USER_NAME");
			
			/* 如果之前没有通过System.setProperty设置HADOOP_USER_NAME，则会返回null。
			 * 这时的HADOOP_USER_NAME应该为本机的hostname
			 * */
			if (user==null) {
				try {
					InetAddress ia = InetAddress.getLocalHost();
					user=ia.getHostName();//获取计算机主机名 
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//更新path，变成类似/user/cephlee/1.txt的形式
			path="/user/"+user+"/"+path;
			return getParentDir(dfs, path);
		}else{
			String parent=file.getParent();
			try {
				int s=getPathXattrs(dfs, parent);
				return getResult(s, parent, dfs);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("没有xattr");
		return false;
	}
	
	public static boolean getResult(int s,String parent,DistributedFileSystem dfs) {
		if (s==0) {//xattr存在且值为true，直接返回true
			System.out.println(parent+"的xattr为true");
			return true;
		}else if (s==1) {
			System.out.println(parent+"的xattr为false");
			return false;
		}else if (s==2) {//xattr不存在或者目录不存在，遍历其父目录
			if (!parent.equals("/")) {
				return getParentDir(dfs, parent);
			}else{//已经遍历到根目录了
				System.out.println(parent+"的xattr不存在或者该目录不存在");
				return false;
			}
		}else if (s==-1) {//返回-1，说明判断不成功,直接返回false
			System.out.println("无法判断");
			return false;
		} 
		return false;
	}
	
	public static int getPathXattrs(DistributedFileSystem dfs,String path) {
		boolean flag=false;
		try {
			//先判断该目录是否存在,存在则获取其xattr
			if (dfs.exists(new Path(path))) {
				//获取父目录的xattr的name列表
				List<String> nameList = dfs.listXAttrs(new Path(path));
				for (int i = 0; i < nameList.size(); i++) {
					if (nameList.get(i).equals("user.zip")) {
						flag=true;
						break;
					}
				}
				
				//通过flag的值，确定xattr是否存在
				if (flag==true) {//xattr存在,只有xattr值为true才能成功
					byte[] value=dfs.getXAttr(new Path(path), "user.zip");
					String strVal=new String(value, "utf-8");
					if (strVal.equals("true")) {//xattr存在且值为true，返回1
						return 0;
					}else if (strVal.equals("false")) {
						return 1;
					}
				}else {//xattr不存在,返回2，需要遍历其父目录
					return 2;
				}
			}else{//目录不存在，直接返回2，需要遍历其上层目录
				return 2;
			}
		} catch (IllegalArgumentException | IOException e) {//有异常，返回-1，说明判断不成功
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return -1;//默认返回-1，说明判断不成功
		
	}
	
	/*测试DistributedFileSystem的xattr*/
	public static void TestXattr(DistributedFileSystem dfs) {
		
		try {
			byte[] value=null;
			String strValue=null;
			
			/*创建xattr并获取*/
			value = "key2.value".getBytes("utf-8");
			dfs.setXAttr(new Path("/test/1.txt"), "user.key2", value);
			System.out.println("hdfs 设置xattr：success");
			value=dfs.getXAttr(new Path("/test/1.txt"), "user.key2");
			strValue=new String(value, "utf-8");
			System.out.println("hdfs getXAttr: <key2.value, "+strValue+">");
			
			/*获取某个文件所有的xattr*/
			Map<String, byte[]> xattrs=dfs.getXAttrs(new Path("/test/1.txt"));
			System.out.println("hdfs getXAttrs(未指定name列表)： ");
			for(Entry<String, byte[]> entry:xattrs.entrySet()){
				System.out.println("<key,value>:"+"<"+entry.getKey()+", "+
						(new String(entry.getValue()))+">");
			}
			
			/*获取指定name列表的xattrs*/
			List<String> names=new ArrayList<>();
			names.add("user.key1");
			names.add("user.key2");
			xattrs=dfs.getXAttrs(new Path("/test/1.txt"), names);
			System.out.println("hdfs getXAttrs(指定name列表)： ");
			for(Entry<String, byte[]> entry:xattrs.entrySet()){
				System.out.println("<key,value>:"+"<"+entry.getKey()+", "+
						(new String(entry.getValue()))+">");
			}
			
			/*获取xattrs的name列表*/
			names=dfs.listXAttrs(new Path("/test/1.txt"));
			System.out.println("hdfs listXAttrs(获取xattr的name列表)： ");
			for (int i = 0; i < names.size(); i++) {
				System.out.println(names.get(i));
			}
			
			/*删除xattr*/
			dfs.removeXAttr(new Path("/test/1.txt"), "user.owner");
			System.out.println("hdfs removeXAttr： success ");
		} catch (IllegalArgumentException  | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*文件上传,这里调用的create()方法，产生的是加密/压缩以后的输出流*/
	public static void UploadFile(DistributedFileSystem dfs,String src,String dst) {
		int byteLen=0;
        byte[] buffer=new byte[1024];
        
		try {
			FSDataOutputStream out=dfs.create(new Path(dst));
			BufferedInputStream in = new BufferedInputStream(new 
		      		FileInputStream(src));
	       
			while((byteLen=in.read(buffer))!=-1){
				 //out.write(buffer); 采用此方法在读取文件末尾数据时，不是写入的读入字节的长度.除非文件大小刚好被BUFFER_SIZE整除
				out.write(buffer, 0, byteLen);//指定每次写入的数据长度为实际读取的数据长度
				//System.out.println(byteLen);
			}
			
			out.flush();
			out.close();
			in.close();
			System.out.println("文件上传完成！");
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	
	 /*文件追加写，原本的DFSClient中append()方法，就是产生加密/压缩的输出流*/
	 public static void AppendFile(DistributedFileSystem dfs,String src,String dst) {
		 //首先检查文件是否存在
		 try {
			 if (!dfs.exists(new Path(dst))) {
				 FSDataOutputStream outNew = dfs.create(new Path(dst));
				 outNew.close();
				}
			 } catch (IllegalArgumentException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		int byteLen=0;
		byte[] buffer=new byte[4096];
		
		try {
			FSDataOutputStream  out=dfs.append(new Path(dst));
			BufferedInputStream in = new BufferedInputStream(new 
		      		FileInputStream(src));

			while ((byteLen = in.read(buffer)) !=-1){
			   	  System.out.println(byteLen);
			   	  out.write(buffer,0,byteLen);
			 }
			  
			  out.flush();
			  out.close();
			  in.close();
			  System.out.println("文件追加写完成！");
		} catch (IllegalArgumentException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
    
	/*文件下载,这里调用的open()方法，产生的是解密/解压缩后的输入流*/
	public static void DownloadFile(DistributedFileSystem dfs,String src,String dst) {
		int byteBuffer=0;
		byte[] buffer=new byte[4096];
		
		try {
			FSDataInputStream fin = dfs.open(new Path(src));
			BufferedOutputStream fout = new BufferedOutputStream(
					new FileOutputStream(dst));
			
			while ((byteBuffer = fin.read(buffer)) !=-1){
				System.out.println(byteBuffer);
				fout.write(buffer,0,byteBuffer);//指定每次写入的数据长度为实际读取的数据长度
			}
			
			fout.flush();
		    fout.close();
		    fin.close();
		    System.out.println("文件已下载！");
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static void TestSimpleFuntions(DistributedFileSystem dfs) {
		System.out.println("HdfsConstants.HDFS_URI_SCHEME: "+dfs.getScheme());
		System.out.println("hdfs URI: "+dfs.getUri().toString());
		System.out.println("hdfs WorkingDirectory: "+dfs.getWorkingDirectory().toString());
		System.out.println("hdfs DefaultBlockSize: "+dfs
				.getDefaultBlockSize()/1024/1024+"MB");
		System.out.println("hdfs DefaultReplication: "+dfs.getDefaultReplication());
		dfs.setWorkingDirectory(new Path("/user/lucy"));
		System.out.println("hdfs HomeDirectory: "+dfs.getHomeDirectory().toString());
		
		try {
			System.out.println("dhdfs toString: "+dfs.toString());
			dfs.setPermission(new Path("/test2"), 
					new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL) );
			System.out.println("hdfs setPermission: success");
			dfs.setOwner(new Path("/test2"), "lucy", "supergroup");
			System.out.println("hdfs setOwner: success");
			dfs.mkdir(new Path("/test/mkdirs"), //该目录的父目录必须存在，否则会报错
					new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			dfs.mkdirs(new Path("/test2/mkdirs"), //该目录的父目录可以不存在
					new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
			dfs.metaSave("metasave.out.txt");
			System.out.println("hdfs metaSave: sucess");
			
			FileStatus[] fileStatus=dfs.listStatus(new Path("/test"));
			System.out.println("hdfs listStatus: ");
			for (int i = 0; i < fileStatus.length; i++) {
				System.out.println(fileStatus[i].toString());
			}
			
			System.out.println("hdfs isInSafeMode: "+dfs.isInSafeMode());
			
			/*如何在一个类中访问另一个类的私有方法*/
			Class<DistributedFileSystem> cls=DistributedFileSystem.class;  
			 //获得私有方法  
	        Method method = cls.getDeclaredMethod("getPathName", Path.class);
	        //设置私有方法可以被访问  
	        method.setAccessible(true);  
	        //调用私有方法  
	        System.out.println("hdfs PathName: "+method.invoke(dfs, 
	        		new Path("/test/gzip")).toString());
	        
	        FileStatus fStatus=dfs.getFileStatus(
					new Path("/test/gzip/WebStorm-2018.1.5.tar.gz"));
			System.out.println(fStatus.toString());
			
		    BlockLocation[] locations1=dfs.getFileBlockLocations(fStatus, 
		    		0, 200*1024*1024);
		    for(int i=0;i<locations1.length;i++){
		    	System.out.println(locations1[i].toString());
		    }
		    
		    BlockLocation[] locations2=dfs.getFileBlockLocations(
		    		new Path("/test/gzip/WebStorm-2018.1.5.tar.gz"), 0, 200*1024*1024);
		    for(int i=0;i<locations2.length;i++){
		    	System.out.println(locations2[i].toString());
		    }
		    
		    boolean isSuccess=dfs.recoverLease(
					new Path("/test/gzip/WebStorm-2018.1.5.tar.gz"));
			System.out.println("hdfs recoverLease: "+isSuccess );
			System.out.println("hdfs setReplication: "+dfs.setReplication(
					new Path("/test/upload.java"), (short)2));
			System.out.println("hdfs createSnapshot: "+dfs.createSnapshot(
					new Path("/snap"),"snap21"));
			/* 报错：java.lang.UnsupportedOperationException: Symlinks not supported
		       dfs.createSymlink(new Path("/test/upload.java"),new Path("/test/symlink"), true);
			   System.out.println("hdfs DefaultReplication: success ");*/
			System.out.println("hdfs delete a not empty dir: "+dfs.delete(
					new Path("/test/mkdirs"), true));
			
			dfs.deleteSnapshot(new Path("/snapshot"), "sn1");
			System.out.println("hdfs deleteSnapshot: success");
			
			AclStatus aclStatus=dfs.getAclStatus(new Path("/test/1.txt"));
			System.out.println("hdfs getAclStatus: "+aclStatus.toString());
			
			System.out.println("hdfs CanonicalServiceName: "+dfs.getCanonicalServiceName());
			
			ContentSummary summary=dfs.getContentSummary(new Path("/test"));
			System.out.println("hdfs getContentSummary: "+summary.toString(true,false));//hOption is false file sizes are returned in bytes
			System.out.println("hdfs getContentSummary: "+summary.toString(true,true));//hOption is true file sizes are returned in human readable 
			System.out.println("hdfs getDefaultPort: "+dfs.getDefaultPort());//ipc port
			
			DiskStatus diskStatus=dfs.getDiskStatus();
			System.out.println("hdfs getDfsUsed: "+diskStatus.getDfsUsed()/1024.0/1024);
			
			System.out.println("hdfs getEZForPath: "+dfs.getEZForPath(new Path("/kms")));
			
			FileChecksum fileChecksum=dfs.getFileChecksum(new Path("/test/1.txt"));
			System.out.println("hdfs getFileChecksum-->getAlgorithmName/hashCode: "+
					fileChecksum.getAlgorithmName()+"/"+fileChecksum.hashCode());
			
			/*报错信息：Path hdfs://192.168.202.34/test/1.txt is not a symbolic link*/			
			/*System.out.println("hdfs getFileLinkStatus-->getSymlink:  "+
					dfs.getFileLinkStatus(new Path("/test/1.txt")).getSymlink());*/
			
			System.out.println("hdfs getInotifyEventStream-->getTxidsBehindEstimate:  "+
					dfs.getInotifyEventStream().getTxidsBehindEstimate());
			/*报错信息：Path hdfs://192.168.202.34/test is not a symbolic link*/			
			System.out.println("hdfs getLinkTarget: "+dfs.getLinkTarget(new Path("/test")));
			System.out.println("hdfs getMissingBlocksCount: "+dfs.getMissingBlocksCount());
			System.out.println("hdfs getServerDefaults-->getBytesPerChecksum: "+
					dfs.getServerDefaults().getChecksumType());
			System.out.println(dfs.getSnapshotDiffReport(new Path("/snap"), 
					"sn02", "snap21").toString());
			
			SnapshottableDirectoryStatus[] dirStatus=dfs.getSnapshottableDirListing();
			for (int i = 0; i < dirStatus.length; i++) {
				System.out.println("hdfs SnapshottableDirectoryStatus-->getFullPath:"+
						dirStatus[i].getFullPath());	
			}
			
			BlockStoragePolicy[] blockStoragePolicy=dfs.getStoragePolicies();
			for (int i = 0; i < blockStoragePolicy.length; i++) {
				System.out.println("hdfs BlockStoragePolicy-->"+
						blockStoragePolicy[i].toString());	
			}
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException 
				| IllegalArgumentException | InvocationTargetException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		
       
		
	}

}
