package com.ameet.tutorial.utils;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class GenerateLargeSequenceFile {
	public static void main(String args[]) throws IOException {
		byte[] bytes = new byte[1024 * 1024];
		Arrays.fill(bytes, (byte)0x01);
		BytesWritable value = new BytesWritable(bytes);
		
		System.out.println("Writing to \"" + args[0] + "\"");
		Configuration conf = new Configuration(true);
		Path path = new Path(args[0]);
		FileSystem fs = path.getFileSystem(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, BytesWritable.class, SequenceFile.CompressionType.NONE);
		IntWritable key = new IntWritable();
		
		for(int i=0; i < 1000; i++) {
			key.set(i);
			writer.append(key,value);
		}
		writer.close();
	}
}
