package com.revature;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.config.MapConfig;
import com.revature.map.USFemEdMapper;

public class USFemEdDriver {
	
	public static void main(String[] args) throws IOException {
		
		if(args.length != 2){
			System.err.println("Usage: FemaleGradUnderThirtyPercent"
					+ " <Input filename> <Output filename>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJobName("US average increase in female education since 2000");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		MapConfig config = new MapConfig();
		config.addCountryCode("USA");
		config.addIndicatorCode("SE.SEC.CUAT.UP.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.BA.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.DO.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.MS.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.ST.FE.ZS");
		config.addYear(1980);
		config.addYearSeries(2000, 2016);
		USFemEdMapper.config = config;
		
		job.setMapperClass(USFemEdMapper.class);
	
	}
}
