package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class AvgEdIncreaseMapper extends Mapper<LongWritable, Text, Text, Text>{	
	private static Logger LOGGER = Logger.getLogger(GenderDataSchemaImpl.class);
	public static Schema SCHEMA = new GenderDataSchemaImpl();
	public static double MAX_VALUE_EXCLUSIVE = 30;
	public static double MIN_VALUE_INCLUSIVE = 0;
	public static List<String> COUNTRIES;
	public static List<String> YEARS;
	public static List<String> TARGET_INDICATOR_CODES;
	public static Map<String, String> INDICATOR_TITLE_MAP;

	static {
		LOGGER.trace("Default static setup");
		if(TARGET_INDICATOR_CODES == null){
			TARGET_INDICATOR_CODES = new ArrayList<>();
			INDICATOR_TITLE_MAP = new HashMap<>();
			YEARS = new ArrayList<>();
			COUNTRIES = new ArrayList<>();

			COUNTRIES.add("United States");

			TARGET_INDICATOR_CODES.add("SE.PRM.CUAT.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.LO.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.UP.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.PO.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.ST.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.BA.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.MS.FE.ZS");

			for(int i = 2000; i <= 2016; i++) {
				YEARS.add(Integer.toString(i));
			}

			INDICATOR_TITLE_MAP.put("SE.PRM.CUAT.FE.ZS", "Female-Primary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.LO.FE.ZS", "Female-Lower-Secondary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.UP.FE.ZS", "Female-Upper-Secondary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.PO.FE.ZS", "Female-Post-Secondary");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.ST.FE.ZS", "Female-Short-Cycle-Ternary");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.BA.FE.ZS", "Female-Bachelor's");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.MS.FE.ZS", "Female-Master's");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.DO.FE.ZS", "Female-Doctoral");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		LOGGER.trace(String.format("map(%s, %s, context)", key.toString(), value.toString()));

		Schema genderSchema = SCHEMA.getNewSchema();
		genderSchema.addRow(value.toString(), "\",\"");
		String indicatorCode = genderSchema.getValueFromColumnName("INDICATOR_CODE");
		String countryName = genderSchema.getValueFromColumnName("COUNTRY_NAME");
		
		//if countries is null then all countries, else countryName must
		// be in the countries list.  
		if(COUNTRIES != null && COUNTRIES.contains(countryName) == false){
			return;
		}
		
		//check if the indicator code does not match.  
		if(TARGET_INDICATOR_CODES.contains(indicatorCode) == false){
			return;
		}
		
		Double percentage;
		String outputValue;
		String indicatorTitle = INDICATOR_TITLE_MAP.get(indicatorCode);
		String outputKey = countryName + ", "+ indicatorTitle ;


		for(String year: YEARS){
			try {
				percentage = Double.valueOf(genderSchema.getValueFromColumnName(year));
			}catch(NumberFormatException e) {
				break;
			}

			if(percentage >= MIN_VALUE_INCLUSIVE && percentage < MAX_VALUE_EXCLUSIVE) {
				outputValue = String.format("%s:%.2f", year, percentage);			
				context.write(new Text(outputKey), 
						new Text(outputValue));
			}
		}

	}
}
