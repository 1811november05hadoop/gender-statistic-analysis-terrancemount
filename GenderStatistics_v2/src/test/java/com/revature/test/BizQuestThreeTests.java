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

import com.revature.BizQuestThree.BizMapper;
import com.revature.BizQuestThree.BizReducer;

public class BizQuestThreeTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver
	<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setup() {

		BizMapper mapper = new BizMapper();

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
				new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"40\",\"\","
						+ "\"50\",\"10\",\"20\""));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2000:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2001:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2002:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2003:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2004:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2005:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2006:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2007:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2008:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2009:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2010:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2011:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2012:0.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2013:12.5"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2014:10.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2015:-80.0"));
		mapDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), new Text("2016:100.0"));

		mapDriver.runTest();
	}

	@Test
	public void validInputReducer() {
		List<Text> iterable = new ArrayList<>();
		iterable.add(new Text("2014:1.0"));
		iterable.add(new Text("2015:2.0"));
		iterable.add(new Text("2016:-1.0"));
		iterable.add(new Text("2017:3.0"));


		reduceDriver.withInput(new Text("United States\tFemale-Education-Secondary-Upper"), iterable);

		reduceDriver.withOutput(new Text("CountryName\tIndicatorTitle\tnumber-of-%-changes\tavg-%-increase\tavg-%-decrease\tmin%\tmax%\tmean%\tmedian%\tstd-dev%\t"
				+ "2000	2001	2002	2003	2004	2005	2006	2007	2008	2009	2010	2011	2012	2013	2014	2015	2016"), 
				new Text(""));
		reduceDriver.withOutput(new Text("United States\tFemale-Education-Secondary-Upper"), 
				new Text("4	2.00	1.00	-1.00	3.00	1.25	1.50	1.71	1.00	2.00	-1.00	3.00"));
		reduceDriver.runTest();
	}

	@Test
	public void validInputMapReduce() {  
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\""
						+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
						+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"40\",\"\","
						+ "\"50\",\"10\",\"20\""));

		mapReduceDriver.withOutput(new Text("CountryName	IndicatorTitle	number-of-%-changes	avg-%-increase	avg-%-decrease	min%	max%"
				+ "	mean%	median%	std-dev%	2000	2001	2002	2003	2004	2005	2006	2007	2008	2009	2010	2011"
				+ "	2012	2013	2014	2015	2016"), 
				new Text(""));
		
		mapReduceDriver.withOutput(new Text("United States	Employment to population ratio, 15+, male (%) (national estimate)"), 
				new Text("4	40.83	80.00	-80.00	100.00	10.63	11.25	73.50	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	0.00	12.50	10.00	-80.00	100.00"));

		mapReduceDriver.runTest();
	}
}








