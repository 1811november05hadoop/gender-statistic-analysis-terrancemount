package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueConcantReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		
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
