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
public class GenderDataByCountryWithYearAndPercentageMapper extends Mapper<LongWritable, Text, Text, Text>{	
	private static Logger LOGGER = Logger.getLogger(GenderDataSchemaImpl.class);
	public static Schema SCHEMA = new GenderDataSchemaImpl();
	public static double MAX_VALUE_EXCLUSIVE = 30;
	public static double MIN_VALUE_INCLUSIVE = 0;
	public static List<String> YEARS;
	public static List<String> TARGET_INDICATOR_CODES;
	public static Map<String, String> INDICATOR_TITLE_MAP;

	static {
		LOGGER.trace("Default static setup");
		if(TARGET_INDICATOR_CODES == null){
			TARGET_INDICATOR_CODES = new ArrayList<>();
			INDICATOR_TITLE_MAP = new HashMap<>();
			YEARS = new ArrayList<>();
			
			TARGET_INDICATOR_CODES.add("SE.PRM.CUAT.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.UP.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.LO.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.SEC.CUAT.PO.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.ST.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.BA.FE.ZS");
			TARGET_INDICATOR_CODES.add("SE.TER.CUAT.MS.FE.ZS");

			for(int i = 1960; i <= 2016; i++) {
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

			INDICATOR_TITLE_MAP.put("SE.PRM.CUAT.MA.ZS", "Male-Primary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.UP.MA.ZS", "Male-Upper-Secondary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.LO.MA.ZS", "Male-Lower-Secondary");
			INDICATOR_TITLE_MAP.put("SE.SEC.CUAT.PO.MA.ZS", "Male-Post-Secondary");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.ST.MA.ZS", "Male-Short-Cycle-Ternary");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.BA.MA.ZS", "Male-Bachelor's");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.BS.MA.ZS", "Male-Master's");
			INDICATOR_TITLE_MAP.put("SE.TER.CUAT.DO.MA.ZS", "Male-Doctoral");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		LOGGER.trace(String.format("map(%s, %s, context)", key.toString(), value.toString()));

		Schema genderSchema = SCHEMA.getNewSchema();
		genderSchema.addRow(value.toString(), "\",\"");
		String indicatorCode = genderSchema.getValueFromColumnName("INDICATOR_CODE");


		if(TARGET_INDICATOR_CODES.contains(indicatorCode)) {
			Double percentage;
			String outputValue;
			String indicatorTitle = INDICATOR_TITLE_MAP.get(indicatorCode);
			String countryName = genderSchema.getValueFromColumnName("COUNTRY_NAME");
			String outputKey = countryName + ", "+ indicatorTitle ;
			//String outputKey = countryName;


			for(String year: YEARS){
				try {
					percentage = Double.valueOf(genderSchema.getValueFromColumnName(year));
				}catch(NumberFormatException e) {
					percentage = null;
				}

				if(percentage != null && percentage >= MIN_VALUE_INCLUSIVE && percentage < MAX_VALUE_EXCLUSIVE) {
					outputValue = String.format("%s, %.2f%%", year, percentage);			
					context.write(new Text(outputKey), 
							new Text(outputValue));
				}
			}
		}
	}
}
