package com.revature.BizQuestFive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class BizDriver{
	private static Logger LOGGER = Logger.getLogger(BizDriver.class);
	
	public static void run(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		LOGGER.trace(String.format("run([%s, %s])", args[0], args[1]));
		

		Job job = new Job();
		
		job.setJobName("Any Indicator code from 1994 to 2004 with greater then 5 % change in the US.");
		job.setJarByClass(BizDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BizMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true); 
		
		System.exit((success) ? 0 : 1);
	}
}
