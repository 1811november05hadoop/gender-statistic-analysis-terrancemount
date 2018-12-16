package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChangeMagnitudeReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> iterableValues, Context context)
		throws IOException, InterruptedException{
		
	}

}
