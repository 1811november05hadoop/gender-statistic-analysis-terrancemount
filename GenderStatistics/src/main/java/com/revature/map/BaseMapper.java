package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.config.MapConfig;
import com.revature.model.Schema;
/**
 * Map country key for countries with over 30% graduation for females.
 * The value will have the level (primary, secondary, post-secondary and graduate level)
 * then the year, then the percentage.
 * 
 * @author Terrance Mount
 *
 */
public class BaseMapper extends Mapper<LongWritable, Text, Text, Text>{	
	public static MapConfig config;

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		Schema schema = config.getSchema();
		schema.putRow(value.toString(), "\",\"");
		
		String indicatorCode = schema.getValueFromColumnName("INDICATOR_CODE");
		if(!config.isValidIndicatorCode(indicatorCode)){
			return;
		}
	
		String countryCode = schema.getValueFromColumnName("COUNTRY_CODE");
		if(!config.isValidCountryCode(countryCode)){
			return;
		}
	
		Double percentage;
		String indicatorTitle = config.getTitleMap().get(indicatorCode);
		String outputKey = schema.getValueFromColumnName("COUNTRY_NAME") + ", "+ indicatorTitle ;
		boolean hasNoPercentage = true;
		
		for(String year: config.getYears()){
			try {
				percentage = Double.valueOf(schema.getValueFromColumnName(year));
			}catch(NumberFormatException e) {
				continue;
			}

			if(config.isInRangeExclusiveMax(percentage))  {
				String outputValue = String.format("%s, %.2f%%", year, percentage);			
				context.write(new Text(outputKey), new Text(outputValue));
				hasNoPercentage = false;
			}
		}
		
		if(hasNoPercentage){
			context.write(new Text(outputKey), new Text("NO RECORDS"));
		}
	}

}
