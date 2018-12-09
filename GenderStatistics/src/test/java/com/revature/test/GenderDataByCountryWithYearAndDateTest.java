package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.GenderDataByCountryWithYearAndPercentageMapper;

public class GenderDataByCountryWithYearAndDateTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
//	private ReduceDriver<Text, Text, Text, Text> redueDriver;
//	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setup() {
		GenderDataByCountryWithYearAndPercentageMapper mapper = 
				new GenderDataByCountryWithYearAndPercentageMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testGenderDataMapper() {
		mapDriver.withInput(new LongWritable(1), 
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"50\",\"16\",\"20\""));
		mapDriver.withOutput(new Text("Arab World"), new Text("2015, 16.00%"));
		mapDriver.withOutput(new Text("Arab World"), new Text("2016, 20.00%"));
		
		mapDriver.runTest();
	}
	
}
