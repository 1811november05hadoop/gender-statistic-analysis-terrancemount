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
public class CountryEdMapper extends Mapper<LongWritable, Text, Text, Text>{	
	public static MapConfig config;

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		Schema genderSchema = config.getNewSchema();
		genderSchema.addRow(value.toString(), "\",\"");

		String indicatorCode = genderSchema.getValueFromColumnName("INDICATOR_CODE");
		if(!config.isValidIndicatorCode(indicatorCode)){
			return;
		}

		String countryName = genderSchema.getValueFromColumnName("COUNTRY_NAME");
		if(!config.isValidCountry(indicatorCode)){
			return;
		}

		Double percentage;
		String indicatorTitle = config.getTitleMap().get(indicatorCode);
		String outputKey = countryName + ", "+ indicatorTitle ;

		for(String year: config.getYears()){
			try {
				percentage = Double.valueOf(genderSchema.getValueFromColumnName(year));
			}catch(NumberFormatException e) {
				break;
			}

			if(config.checkRangeExclusiveMax(percentage)) {
				String outputValue = String.format("%s, %.2f%%", year, percentage);			
				context.write(new Text(outputKey), 
						new Text(outputValue));
			}
		}
	}

}
