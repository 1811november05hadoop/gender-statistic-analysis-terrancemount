package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

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
public class GenderDataByCountryWithYearAndPercentageMapper extends Mapper<LongWritable, Text, Text, Text>{	
	private static Logger LOGGER = Logger.getLogger(GenderDataSchemaImpl.class);
	public static Schema SCHEMA = new GenderDataSchemaImpl();
	private static final List<String> YEARS = new ArrayList<>();
	private static final List<String> TARGET_INDICATOR_CODES = new ArrayList<>();
	
	static {
		LOGGER.trace("Default static setup");
		TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.UP.FE.ZS");
		
		for(int i = 1960; i <= 2016; i++) {
			YEARS.add(Integer.toString(i));
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		LOGGER.trace(String.format("map(%s, %s, context)", key.toString(), value.toString()));
		
		Schema genderSchema = SCHEMA.getNewSchema();
		genderSchema.addRow(value.toString(), "\",\"");

		if(TARGET_INDICATOR_CODES.contains(genderSchema.getValueFromColumnName("INDICATOR_CODE"))) {
			double percentage;
			String output;
			String countryName = genderSchema.getValueFromColumnName("COUNTRY_NAME");

			for(String year: YEARS){
				try {
					percentage = Double.parseDouble(genderSchema.getValueFromColumnName(year));
				}catch(NumberFormatException e) {
					percentage = -1;
				}

				if(percentage != -1 && percentage < 30.0) {
					output = String.format("%s, %.2f%%", year, percentage);			
					context.write(new Text(countryName), 
							new Text(output));
				}
			}
		}
	}
	public static void setIndicatorCodes(String... codes) {
		LOGGER.trace(String.format("setIndicatorCodes(%s)", Arrays.toString(codes)));
		//clear the previous indicator codes if set.
		TARGET_INDICATOR_CODES.clear();

		for(String code: codes) {
			TARGET_INDICATOR_CODES.add(code);
		}
	}

	public static void setSchema(Schema schema) {
		LOGGER.trace(String.format("setSchema(%s)", schema));
		SCHEMA = schema;
	}
	public static void setYears(String... years) {
		LOGGER.trace(String.format("setYears(%s)", Arrays.toString(years)));
		//clear the previous years if set.
		YEARS.clear();
		
		for(String year : years) {
			YEARS.add(year);
		}
	}
	@Override
	public String toString() {
		return "SCHEMA = " + SCHEMA.toString() + 
				" TARGET_CODES = " + TARGET_INDICATOR_CODES.toString() + 
				" YEARS = " + YEARS.toString();
	}
	
	
}
 