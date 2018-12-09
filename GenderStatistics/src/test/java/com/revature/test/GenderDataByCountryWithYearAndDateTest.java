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

import com.revature.map.GenderDataByCountryWithYearAndPercentageMapper;
import com.revature.reduce.GenderDataReducer;


public class GenderDataByCountryWithYearAndDateTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setup() {
		GenderDataByCountryWithYearAndPercentageMapper mapper = 
				new GenderDataByCountryWithYearAndPercentageMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		GenderDataReducer reducer = new GenderDataReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver
				<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testGenderDataMapper() {
		mapDriver.withInput(new LongWritable(1), 
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"50\",\"16\",\"20\""));
		mapDriver.withOutput(new Text("Arab World, Female-Upper-Secondary"), new Text("2015, 16.00%"));
		mapDriver.withOutput(new Text("Arab World, Female-Upper-Secondary"), new Text("2016, 20.00%"));
		
		mapDriver.runTest();
	}
	@Test
	public void testGenderDataReduce() {
		List<Text> iterable = new ArrayList<Text>();
		iterable.add(new Text("2015, 16.00%"));
		iterable.add(new Text("2016, 20.00%"));
		
		reduceDriver.withInput(new Text("Arab World"), iterable);
		reduceDriver.withOutput(new Text("Arab World"), 
				new Text("2015, 16.00% | 2016, 20.00%"));
		reduceDriver.runTest();
	}
	
	@Test
	public void testGenderDataMapReduce() {
		
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
		
		mapReduceDriver.withOutput(new Text("Arab World, Female-Upper-Secondary"), 
				new Text("2015, 16.00% | 2016, 20.00%"));
		
		mapReduceDriver.withOutput(new Text("United States, Female-Upper-Secondary"), 
				new Text("2015, 10.00%"));
		mapReduceDriver.runTest();
		
	}
	
}
