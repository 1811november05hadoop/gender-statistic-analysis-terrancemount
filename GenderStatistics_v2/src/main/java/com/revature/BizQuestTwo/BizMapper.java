package com.revature.BizQuestTwo;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.revature.Util.TableConfig;

public class BizMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	private static Logger LOGGER = Logger.getLogger(BizMapper.class);
	public static final int EARIEST_YEAR = 2000;
	public static TableConfig config;
	static{
		
		if(config == null){
			LOGGER.trace("Loading default configuration");
			config = new TableConfig();
			config.addIndicatorCodeString(""
					+ "SE.PRM.HIAT.FE.ZS,"
					+ "SE.SEC.HIAT.LO.FE.ZS,"
					+ "SE.SEC.HIAT.PO.FE.ZS,"
					+ "SE.SEC.HIAT.UP.FE.ZS,"
					+ "SE.TER.HIAT.DO.FE.ZS,"
					+ "SE.TER.HIAT.BA.FE.ZS,"
					+ "SE.TER.HIAT.DO.FE.ZS,"
					+ "SE.TER.HIAT.MS.FE.ZS,"
					+ "SE.TER.HIAT.ST.FE.ZS,"
					+ "SE.NED.HIAT.FE.ZS,"
					+ "SE.IPR.HIAT.FE.ZS");
			config.addCountryCodes("USA");
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

		processValidRow(config, EARIEST_YEAR, context);
	}

	public static void processValidRow(TableConfig config, int earliestYear,  Context context) 
			throws IOException, InterruptedException{

		int prevYear = -1;
		double prevAmount = 0;

		for(Map.Entry<Integer, Double> entry: config.getValues().entrySet()){
			int year = entry.getKey();
			double amount = entry.getValue();
			
			if(prevYear > 0){
				
				double yearDiff = year - prevYear;
				double diff = (amount - prevAmount)/ yearDiff;

				for(int i = prevYear + 1; i <= year; i++ ){
					if(i >= earliestYear){
						context.write(new Text(config.getCountryName() + "\t" 
								+ config.getShortIndicatorTitleFromRow()),
								new DoubleWritable(diff));
					}
				}
			}
			prevYear = year;
			prevAmount = amount;
		}
	}
}


