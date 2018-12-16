package com.revature;

import java.util.Map;
import java.util.TreeMap;

public class YearDiffTest{

	public static void main(String[] args) {
		
		Map<Integer, Double> yearValues = new TreeMap<>();
		
		yearValues.put(1960, 20.0);
		yearValues.put(1970, 40.0);
		yearValues.put(1974, 50.0);
		yearValues.put(1980, 55.0);
		yearValues.put(1981, 60.0);

		
		int prevYear = -1;
		double prevAmount = 0;
		
		for(Map.Entry<Integer, Double> entry: yearValues.entrySet()){
			if(prevYear > 0){
				int year = entry.getKey();
				double yearDiff = year - prevYear;
				double diff = (entry.getValue() - prevAmount)/ yearDiff;
								
				for(int i = prevYear + 1; i <= year; i++ ){
					System.out.println(i + ":" + diff);
				}
			}
			prevYear = entry.getKey();
			prevAmount = entry.getValue();
		}
		
	
		
	}
}
