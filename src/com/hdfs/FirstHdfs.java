package com.hdfs;

import static org.junit.Assert.*;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FirstHdfs {
//	@Test
//	public void testName() throws Exception {
//		Configuration conf = new Configuration();
//		FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.18.102.66:8020"), conf, "gcl");
////		fileSystem.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\小总结.txt"), new Path("/user/gcl/input/"));
//		fileSystem.copyToLocalFile(new Path("hdfs://172.18.102.66:8020/user/gcl/input/小总结.txt"),
//				new Path("C:\\Users\\Administrator\\Desktop\\小总结1.txt"));
//		System.out.println("完成");
//	}
	@Test
	public void testIo() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.18.102.66:8020"), configuration,"gcl");
		String fileName = "hdfs://172.18.102.66:8020/user/gcl/input/小总结.txt";
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
}
