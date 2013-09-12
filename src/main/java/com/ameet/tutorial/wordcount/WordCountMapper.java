package com.ameet.tutorial.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class WordCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {
 
	private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
	
    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context)
    		throws IOException, InterruptedException {
    	LOG.info("value = " + value + ":withlength:" + value.getLength());
    	String line = value.toString();
    	StringTokenizer tokenizer = new StringTokenizer(line);
    	while (tokenizer.hasMoreTokens()) {
    		word.set(tokenizer.nextToken());
    		context.write(word, ONE);
    	}
    }
}
