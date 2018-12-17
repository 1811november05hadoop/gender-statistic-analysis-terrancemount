package com.revature.Util;

import java.util.Arrays;


public class Statistics {
    Double[] data;
    int size;   
    double averageIncrease;
    double averageDecrease;

    public Statistics(Double[] data) {
        this.data = data;
        size = data.length;
        Arrays.sort(data);
        calculateDiffStats();
    }   

    public void calculateDiffStats(){
    	int numberIncrease = 0;
    	int numberDecrease = 0;
    	double sumIncrease = 0.0;
    	double sumDecrease = 0.0;
    	
    	for(double value: data){
    		if(value > 0){
    			numberIncrease++;
    			sumIncrease += value;
    		} else if(value < 0){
    			numberDecrease++;
    			sumDecrease += value;
    		}
    	}
    	if(numberIncrease > 0){
    		averageIncrease = sumIncrease / numberIncrease;
    	}
    	if(numberDecrease > 0){
    		averageDecrease = sumDecrease / numberDecrease;
    	}
    }
    
    public Double[] getValues(){
    	return data;
    }
    public double getMin(){
    	return data[0];
    }
    public double getMax(){
    	return data[size - 1];
    }
    public double getAverageIncrease(){
    	return averageIncrease;
    }
    public double getAverageDecrease(){
    	return Math.abs(averageDecrease);
    }
    
    public double getMean() {
        double sum = 0.0;
        for(double a : data)
            sum += a;
        return sum/size;
    }

    public double getVariance() {
        double mean = getMean();
        double temp = 0;
        for(double a :data)
            temp += (a-mean)*(a-mean);
        return temp/(size-1);
    }

    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    public double getMedian() {
       if (data.length % 2 == 0)
          return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
       return data[data.length / 2];
    }
}