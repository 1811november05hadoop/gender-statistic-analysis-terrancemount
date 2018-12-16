package com.revature.map;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.configuration.MapConfiguration;


public class CountryPercentageMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(CountryPercentageMapper.class);
	public static final Double CUTOFF =	30.0;
	public static MapConfiguration config;


	static{
		if(config == null){
			config = new MapConfiguration();
			config.addIndicatorCodes("SE.SEC.CUAT.UP.FE.ZS");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		config.addRow(value.toString());

		if(!config.isIndicatorCodeValidInRow()){
			return;
		}
		
		if(!config.isCountryCodeValidInRow()){
			return;
		}
		processValidRow(config, CUTOFF, context);
	}
	
	public static void processValidRow(MapConfiguration config, double cutoff,  Context context) 
			throws IOException, InterruptedException{
		LOGGER.trace(config + ", cutoff = " + cutoff + ", context<not shown>");
		if(config.isValuesEmpty()){
			context.write(new Text("~" + config.getCountryName()),
					new Text("NO RECORDS"));
		}

		for(Map.Entry<Integer, Double> entry: config.getValues().entrySet()){
			if(entry.getValue() < cutoff)  {
				String outputValue = String.format("%s:%.2f%%", entry.getKey(), entry.getValue());			
				context.write(new Text(config.getCountryName()), new Text(outputValue));
			}
		}
	}
}