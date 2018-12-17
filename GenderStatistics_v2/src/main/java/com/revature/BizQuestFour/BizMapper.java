package com.revature.BizQuestFour;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.Util.TableConfig;

public class BizMapper  extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger LOGGER = Logger.getLogger(BizMapper.class);
	public static final int EARIEST_YEAR = 2000;
	public static TableConfig config;


	static{

		if(config == null){
			LOGGER.trace("Loading default configuration");
			config = new TableConfig();
			config.addIndicatorCodes("SL.EMP.TOTL.SP.FE.NE.ZS");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		config.addRow(value.toString());

		if(!config.isIndicatorCodeValidInRow()){
			return;
		}

		if(!config.isCountryCodeValidInRow()){
			return;
		}

		if(config.isValuesEmpty()){
			return;
		}

		processValidRow(config, context);
	}

	private void processValidRow(TableConfig config, Context context) 
			throws IOException, InterruptedException{
		int prevYear = -1;
		double prevAmount = 0;
		int yearDiff = 0;
		double amountDiff = 0;

		for(Map.Entry<Integer, Double> entry: config.getValues().entrySet()){
			int year = entry.getKey();
			double amount = entry.getValue();
			
			if(prevYear < 0){
				amountDiff = 0;
			}else{
				amountDiff = amount - prevAmount;
			}
			yearDiff = year - prevYear;

			while(prevYear + 1 < year){
				prevYear++;
				writeLine(context, prevYear, yearDiff, amountDiff, prevAmount);
				prevAmount += amountDiff;
			}
			writeLine(context, year, yearDiff, amountDiff, prevAmount);
			
			prevYear = year;
			prevAmount = amount;
		} 
	}
	private void writeLine(Context context, int year, int yearDiff, double amountDiff, double prevAmount) 
			throws IOException, InterruptedException{
		double percentDiff;
		if(yearDiff == 0.0 || prevAmount == 0.0){
			percentDiff = 0.0;
		} else {
			percentDiff = amountDiff / yearDiff / prevAmount * 100.0;
		}
		if(year >= EARIEST_YEAR){
			context.write(new Text(config.getCountryName() + "\t" + config.getIndicatorName()),
					new Text(year + ":" + percentDiff));
		}
	}
}



//	private void writePreviousYears(Context context, int year, double diff, TableConfig config) 
//			throws IOException, InterruptedException{
//		
//		while(prevYear < year){
//			if(prevYear >= EARIEST_YEAR){
//				context.write(new Text(config.getCountryName() + "\t" + config.getIndicatorName()),
//						new Text((prevYear) + ":" + (prevAmount + i * diff)));
//			}
//			
//			
//		}
//	}
//}
//
//prevYear = -1;
//prevAmount = 0;
//
//for(Map.Entry<Integer, Double> entry: config.getValues().entrySet()){
//	int year = entry.getKey();
//	double amount = entry.getValue();
//	
//	if(prevYear < 0){
//		prevYear = writePreviousYears(context, year, 0.0, 0.0, config);
//	} 
//	
//	double yearDiff = year - prevYear;
//	double diff = (amount - prevAmount)/ yearDiff;
//
//	writePreviousYears(context, year, prevYear, prevAmount, diff, config);
//
//	context.write(new Text(config.getCountryName() + "\t" + config.getIndicatorName()),
//			new Text(year + ":" + amount));
//	
//	prevYear = year;
//	prevAmount = amount;
//

