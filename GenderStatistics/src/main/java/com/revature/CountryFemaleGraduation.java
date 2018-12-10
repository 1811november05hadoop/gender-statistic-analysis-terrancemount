package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.CountryEdMapper;
import com.revature.reduce.ValueConcantReducer;

public class CountryFemaleGraduation {
	public static void main(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		
		if(args.length != 2){
			System.err.println("Usage: FemaleGradUnderThirtyPercent"
					+ " <Input filename> <Output filename>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(CountryFemaleGraduation.class);
		
		job.setJobName("Find Countries with female graduation under thirty.");
		
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(CountryEdMapper.class);
		job.setReducerClass(ValueConcantReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit((success) ? 0 : 1);
	}
}
