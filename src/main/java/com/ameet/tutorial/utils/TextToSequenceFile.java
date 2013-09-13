package com.ameet.tutorial.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TextToSequenceFile {
	public static void main(String args[]) throws IOException {
		System.out.println("Reading from \"" + args[0] + "\"");
		System.out.println("Writing to \"" + args[1] + "\"");
		Configuration conf = new Configuration(true);
		Path inFilePath = new Path(args[0]);
		Path outFilePath = new Path(args[1]);
		FileSystem fs = inFilePath.getFileSystem(conf);

		FSDataInputStream fdis = fs.open(inFilePath);
		Scanner in = new Scanner(new BufferedReader(new InputStreamReader(fdis)));
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outFilePath, Text.class, IntWritable.class);
		
		Random randomGenerator = new Random();
		Text key = new Text();
		IntWritable value = new IntWritable(); 
		while(in.hasNextLine()) {
			String word = in.nextLine();
			int randInt = randomGenerator.nextInt(2);
			System.out.println(String.format("%s:%d", word, randInt));
			
			key.set(word);
			value.set(randInt);
			writer.append(key,value);
		}
		
		in.close();
		writer.close();
	}
}
