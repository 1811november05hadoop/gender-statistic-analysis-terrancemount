package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.config.MapConfig;
import com.revature.map.CountryFemGradMapper;
import com.revature.model.GenderDataSchemaImpl;
import com.revature.reduce.CountryFemGradReducer;

public class CountryFemGradDriver {
	public static void main(String... args) 
			throws IOException, InterruptedException, ClassNotFoundException{
		
		if(args.length != 2){
			System.err.println("Usage: FemaleGradUnderThirtyPercent"
					+ " <Input filename> <Output filename>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(CountryFemGradDriver.class);
		
		job.setJobName("Find Countries with female graduation under thirty.");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		MapConfig config = new MapConfig();
		config.loadAllYears();
		config.setValueRange(0.0, 30.0);
		config.loadCummulativeEducationTitleMap();
		config.loadFemaleCummulativeEducationCodes();
		config.setSchema(new GenderDataSchemaImpl());
		CountryFemGradMapper.config = config;
		
		job.setMapperClass(CountryFemGradMapper.class);
		job.setReducerClass(CountryFemGradReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit((success) ? 0 : 1);
	}
}
