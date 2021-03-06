package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.config.MapConfig;
import com.revature.model.GenderDataSchemaImpl;
import com.revature.model.Schema;
/**
 * Map country key for countries with over 30% graduation for females.
 * The value will have the level (primary, secondary, post-secondary and graduate level)
 * then the year, then the percentage.
 * 
 * @author Terrance Mount
 *
 */
public class USFemEdMapper extends Mapper<LongWritable, Text, Text, Text>{	
	public static Logger LOGGER = Logger.getLogger(USFemEdMapper.class);
	public static MapConfig config;

	static{
		if(config == null){
			config = new MapConfig();
			config.loadAllYears();
			config.addIndicatorCode("SE.SEC.CUAT.UP.FE.ZS");
			config.setSchema(new GenderDataSchemaImpl());
		}
	}
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		LOGGER.trace(String.format("map(%s, %s, context)", key, value));
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

		String indicatorTitle = config.getTitleMap().get(indicatorCode);
		Double percentage;

		for(String year: config.getYears()){
			try {
				percentage = Double.valueOf(schema.getValueFromColumnName(year));
			}catch(NumberFormatException e) {
				continue;
			}

			String outputValue = String.format("%s:%f", year, percentage);			
			//context.write(new Text(countryCode+":"+indicatorTitle), new Text(outputValue));
			System.out.println(countryCode+":"+indicatorTitle+ "," + outputValue);
		}
	}

}
