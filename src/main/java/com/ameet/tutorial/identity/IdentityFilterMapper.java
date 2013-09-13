package com.ameet.tutorial.identity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
 
public class IdentityFilterMapper {
	private static class FilteredMapper 
		extends Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {

		@Override
		protected void map(IntWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			if(key.get() < 10)
				context.write(key, value);
		}
		
	}
	public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = new Job(conf, "IdentityFilterMapper");
        job.setJarByClass(IdentityFilterMapper.class);
 
        // use filtered mapper and no reducer
        job.setMapperClass(FilteredMapper.class);
        job.setNumReduceTasks(0);
 
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