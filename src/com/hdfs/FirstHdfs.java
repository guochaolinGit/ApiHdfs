package com.hdfs;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FirstHdfs {
	@Test
	public void testName() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:8020"), conf, "gcl");
//		fileSystem.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\小总结.txt"), new Path("/user/gcl/input/"));
		fileSystem.copyToLocalFile(false,new Path("hdfs://hadoop02:8020/user/gcl/input/小总结.txt"),
				new Path("C:\\Users\\Administrator\\Desktop\\小总结1.txt"),true);
		System.out.println("完成");
	}
	@Test
	public void testDownLoadIo() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:8020"), configuration,"gcl");
		String fileName = "hdfs://hadoop02:8020/user/gcl/input/小总结.txt";
		Path path = new Path(fileName);
		FSDataInputStream fsDataInputStream = fileSystem.open(path);
		
		try {
			IOUtils.copyBytes(fsDataInputStream, System.out, 4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(fsDataInputStream);
		}
	}
	
	@Test
	public void testUploadIo() throws Exception {
		Configuration configuration = new Configuration();
//		2019_学习目标.txt
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:8020"), configuration,"gcl");
		String fileName = "E:\\OpenSource\\tools\\hadoop-2.9.2.tar.gz";
		String fileNameOut = "hdfs://hadoop02:8020/user/gcl/input/hadoop-2.9.2.tar.gz";
		Path path = new Path(fileNameOut);
		FileInputStream fileInputStream = new FileInputStream(new File(fileName));
		FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
		
		try {
			IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096);
		} catch (Exception e) {
			IOUtils.closeStream(fileInputStream);
			IOUtils.closeStream(fsDataOutputStream);
		}
	}
	
	@Test
	public void testBlock() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop02:8020"), conf,"gcl");
		
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hdfs://hadoop02:8020/user/gcl/input/hadoop-2.9.2.tar.gz"));
		
		FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\part2"));
		
		fsDataInputStream.seek(1024*1024*128);
		
		IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 1024);
//		byte[] bytes = new byte[1024];
//		for(int i = 0; i < 1024 * 128; i++) {
//			fsDataInputStream.read(bytes);
//			fileOutputStream.write(bytes);
//		}
		
		IOUtils.closeStream(fileOutputStream);
		IOUtils.closeStream(fsDataInputStream);
	}
}
