package com.revature.reduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CountryPercentageReducer extends Reducer<Text, Text, Text, Text> {
	public static Logger LOGGER = Logger.getLogger(CountryPercentageReducer.class);
	
	public void reduce(Text key, Iterable<Text> iterableValues, Context context)
			throws IOException, InterruptedException{
		LOGGER.trace(String.format("reduce(%s, %s, context)", key.toString(), iterableValues));
		
		SortedSet<String> values = new TreeSet<>(Comparator.reverseOrder());
		
		for(Text value: iterableValues){
			values.add(value.toString());
		}
		
		StringBuilder output = new StringBuilder();
		
		int i = 0;
		for(String value: values){
			if(i++ != 0) {
				output.append(", ");
			}
			output.append(value);
		}
		context.write(key, new Text(output.toString()));
	}
}