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

import com.revature.BizQuestTwo.BizMapper;
import com.revature.BizQuestTwo.BizReducer;

public class BizQuestTwoTests {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	
	@Before
	public void setup() {

		BizMapper mapper = new BizMapper();
		
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		BizReducer reducer = new BizReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver
				<LongWritable, Text, Text, DoubleWritable, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void validInputMapper() {
		mapDriver.withInput(new LongWritable(1), 
				new Text("\"United States\",\"USA\",\"Educational attainment, "
				+ "at least completed upper secondary, population 25+, "
				+ "female (%) (cumulative)\",\"SE.SEC.HIAT.UP.FE.ZS\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"40\",\"\","
				+ "\"50\",\"10\",\"20\""));

		mapDriver.withOutput(new Text("United States	Female-Education-Secondary-Upper"), new DoubleWritable(5));
		mapDriver.withOutput(new Text("United States	Female-Education-Secondary-Upper"), new DoubleWritable(5));
		mapDriver.withOutput(new Text("United States	Female-Education-Secondary-Upper"), new DoubleWritable(-40));
		mapDriver.withOutput(new Text("United States	Female-Education-Secondary-Upper"), new DoubleWritable(10));
	
		mapDriver.runTest();
	}
	
	@Test
	public void validInputReducer() {
		List<DoubleWritable> iterable = new ArrayList<>();
		iterable.add(new DoubleWritable(1.0));
		iterable.add(new DoubleWritable(2.0));
		iterable.add(new DoubleWritable(3.0));
		iterable.add(new DoubleWritable(4.0));
		iterable.add(new DoubleWritable(-1.0));
		
		
		reduceDriver.withInput(new Text("United States\tFemale-Education-Secondary-Upper"), iterable);

		reduceDriver.withOutput(new Text("CountryName	IndicatorTitle	number-of-changes	avg-increase	avg-decrease	min	max	mean	median	std-dev"), 
				new Text(""));
		reduceDriver.withOutput(new Text("United States\tFemale-Education-Secondary-Upper"), 
				new Text("5	2.50	1.00	-1.00	4.00	1.80	2.00	1.92"));
		reduceDriver.runTest();
	}
	
	@Test
	public void validInputMapReduce() {  
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("\"United States\",\"USA\",\"Educational attainment, "
						+ "at least completed upper secondary, population 25+, "
						+ "female (%) (cumulative)\",\"SE.SEC.HIAT.UP.FE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"40\",\"\","
						+ "\"50\",\"10\",\"20\""));
		
		mapReduceDriver.withOutput(new Text("CountryName	IndicatorTitle	number-of-changes	avg-increase	avg-decrease	min	max	mean	median	std-dev"), 
				new Text(""));
		mapReduceDriver.withOutput(new Text("United States\tFemale-Education-Secondary-Upper"), 
				new Text("4	6.67	40.00	-40.00	10.00	-5.00	5.00	23.45"));

		mapReduceDriver.runTest();
	}
}








