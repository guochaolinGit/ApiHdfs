package com.gcl.practice.hdfs;

import static org.junit.Assert.*;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class FirstHdfs {

	@Test
	public void testFirst() throws Exception {
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://172.18.102.66:8020");
		FileSystem fs = FileSystem.get(new URI("hdfs://172.18.102.66:8020"),conf,"gcl");
		Path path = new Path("/Users/guochaolin/Desktop/小总结.txt");
		Path path2 = new Path("/user/gcl/input/小总结.txt");
//		fs.copyFromLocalFile(true,path,path2 );
		fs.copyToLocalFile(path2,path);
		fs.close();
	}
}
