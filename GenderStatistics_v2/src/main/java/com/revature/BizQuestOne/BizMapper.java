package com.revature.BizQuestOne;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.Util.TableConfig;


public class BizMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(BizMapper.class);
	public static final Double CUTOFF =	30.0;
	public static TableConfig config;


	static{
		if(config == null){
			config = new TableConfig();
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

	public static void processValidRow(TableConfig config, double cutoff,  Context context) 
			throws IOException, InterruptedException{
		LOGGER.trace(config + ", cutoff = " + cutoff + ", context<not shown>");
		if(config.isValuesEmpty()){
			context.write(new Text("~" + config.getCountryName()),
					new Text("NO RECORDS"));
		}
		Map<Integer, Double> values = config.getValues();
		int i = 1;
		for(Map.Entry<Integer, Double> entry: values.entrySet()){
			if(i++ == values.size()){
				if(entry.getValue() < cutoff)  {
					String outputValue = String.format("%s:%.2f%%", entry.getKey(), entry.getValue());			
					context.write(new Text(config.getCountryName()), new Text(outputValue));
				}
			}
		}
	}
}