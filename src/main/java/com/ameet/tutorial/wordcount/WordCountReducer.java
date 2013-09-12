package com.ameet.tutorial.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class WordCountReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(WordCountReducer.class);

    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    	int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
    	LOG.info("key="+text+":withlength:"+text.getLength()+":andsum:"+sum);
        context.write(text, new IntWritable(sum));
    }
}
