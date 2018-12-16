package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.ChangeMagnitudeMapper;

public class ChangeMagnatudeTests {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver
		<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	
	@Before
	public void setup() {

		ChangeMagnitudeMapper mapper = 
				new ChangeMagnitudeMapper();
		
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
//		CountryPercentageReducer reducer = new CountryPercentageReducer();
//		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Text>();
//		reduceDriver.setReducer(reducer);
//		
//		mapReduceDriver = new MapReduceDriver
//				<LongWritable, Text, Text, DoubleWritable, Text, Text>();
//		//mapReduceDriver.setMapper(mapper);
//		mapReduceDriver.setReducer(reducer);
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

		mapDriver.withOutput(new Text("United States, Female-Education-Secondary-Upper"), new DoubleWritable(5));
		mapDriver.withOutput(new Text("United States, Female-Education-Secondary-Upper"), new DoubleWritable(5));
		mapDriver.withOutput(new Text("United States, Female-Education-Secondary-Upper"), new DoubleWritable(-40));
		mapDriver.withOutput(new Text("United States, Female-Education-Secondary-Upper"), new DoubleWritable(10));
	
		mapDriver.runTest();
	}
}
