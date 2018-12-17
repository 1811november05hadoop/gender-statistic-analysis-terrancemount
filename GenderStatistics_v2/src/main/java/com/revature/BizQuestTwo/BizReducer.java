package com.revature.BizQuestTwo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.revature.Util.Statistics;

public class BizReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(BizReducer.class);
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		//context.write(new Text(context.getJobName()), new Text(""));
		String head = "CountryName\tIndicatorTitle\tnumber-of-changes\tavg-increase\tavg-decrease"
				+ "\tmin\tmax\tmean\tmedian\tstd-dev";
		
		context.write(new Text(head), new Text(""));
	}
	
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> iterableValues, Context context)
		throws IOException, InterruptedException{
		
		List<Double> values = new ArrayList<>();
		
		for(DoubleWritable value: iterableValues){
			values.add(value.get());
		}
		
		LOGGER.trace(values);
		if(values.size() == 1){
			double value = values.get(0);
			if(value < 0){
				context.write(key, new Text(String.format("only-one-decrease:%.2f", Math.abs(value))));
			} else if(value > 0){
				context.write(key, new Text(String.format("only-one-increase:%.2f", Math.abs(value))));
			} else {
				context.write(key, new Text("no-change"));
			}
			return;
		}
		
		Statistics stats = new Statistics(values.toArray(new Double[0]));
	
		context.write(key, new Text(String.format("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f", 
				values.size(), stats.getAverageIncrease(),stats.getAverageDecrease(), stats.getMin(), 
				stats.getMax(), stats.getMean(), stats.getMedian(), stats.getStdDev())));
		
	}
}
