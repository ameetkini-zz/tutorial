package com.ameet.tutorial.identity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
 
public class Identity {
	
	public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = new Job(conf, "Identity");
        job.setJarByClass(Identity.class);
 
        // use identity mapper/reducer
        job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
 
}