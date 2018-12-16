package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


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
public class CountryFemGradDoubleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{	
	public static MapConfig config;

	static{
		if(config == null){
			config = new MapConfig();
			config.loadAllYears();
			config.setValueRange(0.0, 30.0);
			config.addIndicatorCode("SE.SEC.CUAT.UP.FE.ZS");
			config.setSchema(new GenderDataSchemaImpl());
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		Schema schema = config.getSchema();
		schema.clearRow();
		schema.putRow(value.toString(), "\",\"");
		
		String indicatorCode = schema.getValueFromColumnName("INDICATOR_CODE");
		if(!config.isValidIndicatorCode(indicatorCode)){
			return;
		}

		double doubleValue;
		boolean isBelow = false;
		List<Double> values = new ArrayList<>();
		
		for(String year: config.getYears()){
		
			doubleValue = Double.parseDouble(schema.getValueFromColumnName(year));
		
			
			
			values.add(doubleValue);
			if(!isBelow && !config.isInRangeExclusiveMax(doubleValue)){
				isBelow = true;
			}
		}
		
		String countryName = schema.getValueFromColumnName("COUNTRY_NAME");
		if(isBelow){
			
			for(Double outputValue: values){
				context.write(new Text(countryName), new DoubleWritable(outputValue.doubleValue()));
			}
		} else if(values.size() == 0){
			context.write(new Text("~" + countryName), new DoubleWritable(-1.0));
		}
		
	}

}
