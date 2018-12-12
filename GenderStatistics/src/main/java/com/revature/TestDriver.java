package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.revature.config.MapConfig;
import com.revature.map.CountryFemGradMapper;
import com.revature.map.USFemEdMapper;
import com.revature.model.GenderDataSchemaImpl;

public class TestDriver {
	public static Logger LOGGER = Logger.getLogger(TestDriver.class);
	public static void main(String[] args) throws IOException, InterruptedException {
		LOGGER.trace("main()");
		//testCountryFemMapper();
		testUSFemEdDriver();
		
		
		
		
		
	}
	
	
	public static void testCountryFemMapper() throws IOException, InterruptedException{
		LOGGER.trace("testCountryFemMapper()");
		CountryFemGradMapper mapper = new CountryFemGradMapper();
		MapConfig config = new MapConfig();
		config.loadAllYears();
		config.setValueRange(0.0, 30.0);
		config.loadCummulativeEducationTitleMap();
		config.loadFemaleCummulativeEducationCodes();
		config.setSchema(new GenderDataSchemaImpl());
		CountryFemGradMapper.config = config;
		
		Text VALID_MAPPER_INPUT_1 = new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
				+ "at least completed upper secondary, population 25+, "
				+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"50\",\"16\",\"20\"");
		
		mapper.map(new LongWritable(1), VALID_MAPPER_INPUT_1, null);
	}
	
	public static void testUSFemEdDriver() throws IOException, InterruptedException{
		LOGGER.trace("testUSFemDriver()");
		MapConfig config = new MapConfig();
		config.addCountryCode("USA");
		config.addIndicatorCode("SE.SEC.HIAT.UP.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.BA.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.DO.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.MS.FE.ZS");
		config.addIndicatorCode("SE.TER.HIAT.ST.FE.ZS");
		config.loadCompleteOnlyEducationTitleMap();
		config.addYear(1980);
		config.addYearSeries(2000, 2016);
		config.setSchema(new GenderDataSchemaImpl());
		USFemEdMapper.config = config;
		
		USFemEdMapper mapper = new USFemEdMapper();
		
		Text VALID_MAPPER_INPUT_1 = new Text("\"United States\",\"USA\",\"Educational attainment, "
				+ "at least completed upper secondary, population 25+, "
				+ "female (%) (cumulative)\",\"SE.SEC.HIAT.UP.FE.ZS\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"50\",\"16\",\"20\"");
		
		mapper.map(new LongWritable(1), VALID_MAPPER_INPUT_1, null);
	}
	
}
