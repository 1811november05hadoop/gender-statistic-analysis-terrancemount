package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class DoubleAvgReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	public static Logger LOGGER = Logger.getLogger(DoubleAvgReducer.class);
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException{
		LOGGER.trace(String.format("reduce(%s, %s, context)", key.toString(), values));
		
		double sum = 0;
		int count = 0;
		for(DoubleWritable value: values) {
			if(count == 0 && value.get() == -1){
				context.write(key, new Text("NO RECORD"));
				return;
			}
			
			sum += value.get();
			count++;
		}
	
		context.write(key, new Text(String.format("avg: %.2f%%", sum / count)));
		
	}
}