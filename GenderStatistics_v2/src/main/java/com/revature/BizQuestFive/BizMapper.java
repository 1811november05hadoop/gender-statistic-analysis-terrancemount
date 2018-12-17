package com.revature.BizQuestFive;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.Util.TableConfig;

public class BizMapper  extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(BizMapper.class);
	public static final int EARIEST_YEAR = 2000;
	public static TableConfig config;


	static{

		if(config == null){
			LOGGER.trace("Loading default configuration");
			config = new TableConfig();
			config.addCountryCodes("USA");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		config.addRow(value.toString());

		if(!config.isCountryCodeValidInRow()){
			return;
		}
		
		Set<String> parts = new HashSet<>();
		String[] splits = config.getIndicatorCode().split("[.]");
		for(String split: splits){
			parts.add(split);
		}
		
		if(parts.contains("MA")){
			return;
		}
		
		if(config.isValuesEmpty()){
			return;
		}

		processValidRow(config, context);
	}

	private void processValidRow(TableConfig config, Context context) 
			throws IOException, InterruptedException{
		Map<Integer, Double> map = config.getValues();
		
		Double amt1994 = map.get(1994);
		
		Double amt2004 = map.get(2004);
		
		if(amt1994 == null || amt2004 == null){
			return;
		}
		
		double diff = amt2004 - amt1994;
		double percentChange = diff / amt1994 * 100;
		if(Math.abs(percentChange) > 10){
			context.write(new Text(config.getIndicatorName()), new Text(String.format("%.2f", percentChange)));
		}
	}
}


