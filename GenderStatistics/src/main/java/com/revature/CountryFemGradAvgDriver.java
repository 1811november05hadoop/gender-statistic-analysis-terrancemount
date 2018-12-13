package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.CountryFemGradDoubleMapper;
import com.revature.reduce.DoubleAvgReducer;

public class CountryFemGradAvgDriver {
	public static void main(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		if(args.length != 2){
			System.err.println("Usage: FemaleGradUnderThirtyPercent"
					+ "<Input filename> <Output filename>");
			System.exit(-1);
		}

		Job job = new Job();
		
		job.setJobName("Find Countries with women graduation under thirty percent.");
		job.setJarByClass(CountryFemGradAvgDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CountryFemGradDoubleMapper.class);
		job.setReducerClass(DoubleAvgReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit((success) ? 0 : 1);
	}
}
