package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.GenderDataByCountryWithYearAndPercentageMapper;
import com.revature.reduce.GenderDataReducer;

public class GradPercent {
	public static void main(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		
		if(args.length != 2){
			System.err.println("Usage: FemaleGradUnderThirtyPercent"
					+ " <Input filename> <Output filename>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(GradPercent.class);
		
		job.setJobName("Find Countries with female graduation under thirty.");
		
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(GenderDataByCountryWithYearAndPercentageMapper.class);
		job.setReducerClass(GenderDataReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit((success) ? 0 : 1);
	}
}
