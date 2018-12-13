package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.config.MapConfig;
import com.revature.map.CountryFemGradTextMapper;
import com.revature.model.GenderDataSchemaImpl;
import com.revature.reduce.DoubleAvgReducer;


public class CountryFemaleEducationTests {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	
//	@Before
//	public void setup() {
//		MapConfig config = new MapConfig();
//		config.loadAllYears();
//		config.setValueRange(0.0, 30.0);
//		config.addIndicatorCode("SE.SEC.CUAT.UP.FE.ZS");
//		config.setSchema(new GenderDataSchemaImpl());
//		
//		CountryFemGradDoubleMapper mapper = 
//				new CountryFemGradDoubleMapper();
//		
//		//set the static config mapper
//		CountryFemGradDoubleMapper.config = config;
//		
//		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
//		mapDriver.setMapper(mapper);
//		
//		DoubleAvgReducer reducer = new DoubleAvgReducer();
//		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Text>();
//		reduceDriver.setReducer(reducer);
//		
//		mapReduceDriver = new MapReduceDriver
//				<LongWritable, Text, Text, DoubleWritable, Text, Text>();
//		mapReduceDriver.setMapper(mapper);
//		mapReduceDriver.setReducer(reducer);
//	}
//	
//	@Test
//	public void validInputMapper() {
//		mapDriver.withInput(new LongWritable(1), 
//				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
//				+ "at least completed upper secondary, population 25+, "
//				+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
//				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//				+ "\"50\",\"16\",\"20\""));
//		mapDriver.withOutput(new Text("Arab World"), new DoubleWritable(50.0));
//		mapDriver.withOutput(new Text("Arab World"), new DoubleWritable(16.0));
//		mapDriver.withOutput(new Text("Arab World"), new DoubleWritable(20.0));
//
//		mapDriver.runTest();
//	}
//	
//	@Test
//	public void validInputNoRecordsMapper(){
//		mapDriver.withInput(new LongWritable(1), 
//				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
//						+ "at least completed upper secondary, population 25+, "
//						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
//						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\""));
//		mapDriver.withOutput(new Text("Arab World"), 
//				new DoubleWritable(-1.0));
//	}
//	
//	@Test
//	public void validInputReducer() {
//		List<DoubleWritable> iterable = new ArrayList<>();
//		iterable.add(new DoubleWritable(16.0));
//		iterable.add(new DoubleWritable(20.0));
//		
////		List<DoubleWritable> iterable2 = new ArrayList<>();
////		iterable2.add(new DoubleWritable(-1.0));
//		
//		reduceDriver.withInput(new Text("Arab World"), iterable);
////		reduceDriver.withInput(new Text("Iceland"), iterable2);
//		reduceDriver.withOutput(new Text("Arab World"), 
//				new Text("avg: 18.00%"));
////		reduceDriver.withOutput(new Text("Iceland"), 
////				new Text("NO RECORD"));
//		reduceDriver.runTest();
//	}
//	@Test
//	public void validInputNoRecordReducer(){
//
//		
//		List<DoubleWritable> iterable2 = new ArrayList<>();
//		iterable2.add(new DoubleWritable(-1.0));
//		
//
//		reduceDriver.withInput(new Text("Iceland"), iterable2);
//	
//		reduceDriver.withOutput(new Text("Iceland"), 
//				new Text("NO RECORD"));
//		reduceDriver.runTest();
//	}
//	
//	@Test
//	public void validInputMapReduce() {
//		//correct indicator code with two correct values.  
//		mapReduceDriver.withInput(new LongWritable(1), 
//				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
//						+ "at least completed upper secondary, population 25+, "
//						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
//						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"50\",\"16\",\"20\""));
//		
//		//correct indicator code, one value and a different country
//		mapReduceDriver.withInput(new LongWritable(2), 
//				new Text("\"United States\",\"USA\",\"Educational attainment, "
//						+ "at least completed upper secondary, population 25+, "
//						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
//						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"50\",\"10\",\"60\""));
//		 
//
//		
//		mapDriver.withInput(new LongWritable(3), 
//				new Text("\"Iceland\",\"ARB\",\"Educational attainment, "
//						+ "at least completed upper secondary, population 25+, "
//						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
//						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\""));
//		
//		//incorrect indicator code with correct values
//		mapReduceDriver.withInput(new LongWritable(4), 
//				new Text("\"United States\",\"ARB\",\"Educational attainment, "
//						+ "at least completed upper secondary, population 25+, "
//						+ "female (%) (cumulative)\",\"SE.PRM.CUAT.FE.ZS\""
//						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
//						+ "\"3\",\"10\",\"6\""));
//		
//		mapReduceDriver.withOutput(new Text("Arab World"), 
//				new Text("avg: 28.67%"));
//		mapReduceDriver.withOutput(new Text("United States"), 
//				new Text("avg: 40.00%"));
//		
//
//		mapReduceDriver.runTest();
//	}
}
