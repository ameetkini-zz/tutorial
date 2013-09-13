package com.ameet.tutorial.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class PrintKeys {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration(true);
		Path path = new Path(args[0]);
		FileSystem fs = path.getFileSystem(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		IntWritable key = new IntWritable();
		while(reader.next(key)) {
			System.out.println("key = " + key);
		}
		reader.close();
	}
}
