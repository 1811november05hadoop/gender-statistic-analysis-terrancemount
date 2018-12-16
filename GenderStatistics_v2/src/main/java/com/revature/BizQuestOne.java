package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.revature.map.CountryPercentageMapper;
import com.revature.reduce.CountryPercentageReducer;

public class BizQuestOne{
	private static Logger LOGGER = Logger.getLogger(BizQuestOne.class);
	
	public static void run(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		LOGGER.trace(String.format("run([%s, %s])", args[0], args[1]));
		

		Job job = new Job();
		
		job.setJobName("Find Countries with women graduation under thirty percent.");
		job.setJarByClass(BizQuestOne.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CountryPercentageMapper.class);
		job.setReducerClass(CountryPercentageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true); 
		
		System.exit((success) ? 0 : 1);
	}
}
