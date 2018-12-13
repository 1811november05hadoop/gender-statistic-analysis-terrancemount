package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ConcantReducer extends Reducer<Text, Text, Text, Text> {
	public static Logger LOGGER = Logger.getLogger(ConcantReducer.class);
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		LOGGER.trace(String.format("reduce(%s, %s, context)", key.toString(), values));
		
		StringBuilder output = new StringBuilder();
		
		int i = 0;
		for(Text value: values) {
			if(i++ != 0) {
				output.append(" | ");
			}
			output.append(String.format("%s", value.toString()));
		}
		context.write(key, new Text(output.toString()));
	}
	
}