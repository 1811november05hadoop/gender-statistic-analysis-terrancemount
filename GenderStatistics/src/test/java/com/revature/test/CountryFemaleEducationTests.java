package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.config.MapConfig;
import com.revature.map.CountryFemGradMapper;
import com.revature.model.GenderDataSchemaImpl;
import com.revature.reduce.CountryFemGradReducer;


public class CountryFemaleEducationTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	public static final Text VALID_MAPPER_INPUT_1 = new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
			+ "at least completed upper secondary, population 25+, "
			+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"," 
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
			+ "\"50\",\"16\",\"20\"");
	public static final Text VALID_MAPPER_OUTPUT_1_KEY = new Text("Arab World, Female-Upper-Secondary");
	public static final Text VALID_MAPPER_OUTPUT_1_VALUE_1 = new Text("2015, 16.00%");
	public static final Text VALID_MAPPER_OUTPUT_1_VALUE_2 = new Text("2016, 20.00%");
	
	
	@Before
	public void setup() {
		MapConfig config = new MapConfig();
		config.loadAllYears();
		config.setValueRange(0.0, 30.0);
		config.loadCummulativeEducationTitleMap();
		config.loadFemaleCummulativeEducationCodes();
		config.setSchema(new GenderDataSchemaImpl());
		
		CountryFemGradMapper mapper = 
				new CountryFemGradMapper();
		
		//set the static config mapper
		CountryFemGradMapper.config = config;
		
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		CountryFemGradReducer reducer = new CountryFemGradReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver
				<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void validInputMapper() {
		mapDriver.withInput(new LongWritable(1), VALID_MAPPER_INPUT_1);
		mapDriver.withOutput(VALID_MAPPER_OUTPUT_1_KEY, VALID_MAPPER_OUTPUT_1_VALUE_1);
		mapDriver.withOutput(VALID_MAPPER_OUTPUT_1_KEY, VALID_MAPPER_OUTPUT_1_VALUE_2);
		
		mapDriver.runTest();
	}
	
	@Test
	public void validInputNoRecordsMapper(){
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\""));
		mapDriver.withOutput(new Text("Arab World, Female-Upper-Secondary"), 
				new Text("NO RECORDS"));
	}
	
	@Test
	public void validInputReducer() {
		List<Text> iterable = new ArrayList<Text>();
		iterable.add(new Text("2015, 16.00%"));
		iterable.add(new Text("2016, 20.00%"));
		
		reduceDriver.withInput(new Text("Arab World"), iterable);
		reduceDriver.withOutput(new Text("Arab World"), 
				new Text("2015, 16.00% | 2016, 20.00%"));
		reduceDriver.runTest();
	}
	
	@Test
	public void validInputMapReduce() {
		//correct indicator code with two correct values.  
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"50\",\"16\",\"20\""));
		
		//correct indicator code, one value and a different country
		mapReduceDriver.withInput(new LongWritable(2), 
				new Text("\"United States\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"50\",\"10\",\"60\""));
		 
		//incorrect indicator code with correct values
		mapReduceDriver.withInput(new LongWritable(3), 
				new Text("\"United States\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.MA.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"3\",\"10\",\"6\""));
		
		//incorrect indicator code with correct values
		mapReduceDriver.withInput(new LongWritable(4), 
				new Text("\"United States\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"3\",\"10\",\"6\""));
	
		mapReduceDriver.withOutput(new Text("Arab World, Female-Upper-Secondary"), 
				new Text("2015, 16.00% | 2016, 20.00%"));
		
		mapReduceDriver.withOutput(new Text("United States, Female-Primary"), 
				new Text("2014, 3.00% | 2015, 10.00% | 2016, 6.00%"));
		mapReduceDriver.withOutput(new Text("United States, Female-Upper-Secondary"), 
				new Text("2015, 10.00%"));
		mapReduceDriver.runTest();
	}
}
