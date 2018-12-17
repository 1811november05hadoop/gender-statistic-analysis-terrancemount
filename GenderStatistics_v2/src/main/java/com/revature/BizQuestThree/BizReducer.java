package com.revature.BizQuestThree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.revature.Util.Statistics;

public class BizReducer extends Reducer<Text, Text, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(BizReducer.class);
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		StringBuilder head = new StringBuilder(); 
				head.append("CountryName\tIndicatorTitle\tnumber-of-%-changes\tavg-%-increase\t");
				head.append("avg-%-decrease\tmin%\tmax%\tmean%\tmedian%\tstd-dev%");
				
				for(int i = com.revature.BizQuestThree.BizMapper.EARIEST_YEAR; i <= 2016; i++ ){
					head.append("\t").append(i);
				}
				
		context.write(new Text(head.toString()), new Text(""));
	}
	
	
	@Override
	public void reduce(Text key, Iterable<Text> iterableValues, Context context)
		throws IOException, InterruptedException{
		LOGGER.trace("reducing("+ key.toString()+")");
		
		SortedSet<String> sortedYearAmounts = new TreeSet<>();
		
		for(Text value: iterableValues){
			sortedYearAmounts.add(value.toString());
		}
		
		
		List<Double> amounts = generateAmountsList(sortedYearAmounts);
		
		if(amounts.size()==0){
			return;
		}
		
		Statistics stats = new Statistics(amounts.toArray(new Double[0]));
	
		StringBuilder strValues = new StringBuilder();
		
		for(String strAmount: sortedYearAmounts){
			double amount = Double.parseDouble(strAmount.split(":")[1]);
			strValues.append("\t").append(String.format("%.2f", amount));
		}
		
		context.write(key, new Text(String.format("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f%s",
				amounts.size(), stats.getAverageIncrease(),stats.getAverageDecrease(), 
				stats.getMin(), stats.getMax(), stats.getMean(), stats.getMedian(), stats.getStdDev(), strValues.toString())));
	}
	private List<Double> generateAmountsList(SortedSet<String> sortedYearAmounts){
		List<Double> amounts = new ArrayList<>();
		boolean hasNoValidAmountBeenFound = true;
		
		for(String yearAmount: sortedYearAmounts){
			String[] split = yearAmount.split(":");
			Double amount = Double.valueOf(split[1]);
		
			if(hasNoValidAmountBeenFound){
				if(amount == 0){
					continue;
				}
				hasNoValidAmountBeenFound = false;
			}
			amounts.add(amount);
		}
		return amounts;
	}
}
