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

import com.revature.BizQuestOne.BizMapper;
import com.revature.BizQuestOne.BizReducer;


public class BizQuestOneTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setup() {

		BizMapper mapper = 
				new BizMapper();
		
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		BizReducer reducer = new BizReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver
				<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void validInputMapper() {
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
		mapDriver.withOutput(new Text("Arab World"), new Text("2016:20.00%"));

		mapDriver.runTest();
	}
	
	@Test
	public void validInputNoRecordsMapper(){
		mapDriver.withInput(new LongWritable(1), 
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\""));
		mapDriver.withOutput(new Text("Arab World"), new Text("NO RECORDS"));
	}
	
	@Test
	public void validInputReducer() {
		List<Text> iterable = new ArrayList<>();
		iterable.add(new Text("2015:16.00%"));
		iterable.add(new Text("2016:20.00%"));
		
		
		reduceDriver.withInput(new Text("Arab World"), iterable);

		reduceDriver.withOutput(new Text("Arab World"), 
				new Text("2016:20.00%, 2015:16.00%"));
		reduceDriver.runTest();
	}
	@Test
	public void validInputNoRecordReducer(){
		List<Text> iterable = new ArrayList<>();
		iterable.add(new Text("NO RECORDS"));
		reduceDriver.withInput(new Text("Iceland"), iterable);
	
		reduceDriver.withOutput(new Text("Iceland"), 
				new Text("NO RECORDS"));
		reduceDriver.runTest();
	}
	
	@Test
	public void validInputMapReduce() {  
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
	
		
		mapReduceDriver.withOutput(new Text("Arab World"), 
				new Text("2016:20.00%"));
		

		mapReduceDriver.runTest();
	}
}
