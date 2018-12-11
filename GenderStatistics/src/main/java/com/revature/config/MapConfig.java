package com.revature.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.revature.model.Schema;

public class MapConfig {
	private Schema schema;
	private boolean isRangeUsed = false;
	private double maxValue = Double.MAX_VALUE;
	private double minValue = Double.MIN_VALUE;
	private Set<String> countryCodes = new HashSet<>();
	private List<String> years = new ArrayList<>();
	private Set<String> indicatorCodes = new HashSet<>();
	private Map<String, String> titleMap = new HashMap<>();
	
	/**
	 * Gets a new instance of the schema to manipulate in the 
	 * mapper.  
	 * @return a copy of the schema.  
	 */
	public Schema getNewSchema(){
		return schema.getNewSchema();
	}
	
	/**
	 * Sets the min and max value allowed.
	 * @param minValue a double for the min range
	 * @param maxValue a double for the max range.  
	 */
	public void setValueRange(Double minValue, Double maxValue){
		this.minValue = minValue;
		this.maxValue = maxValue;
		isRangeUsed = true;
	}
	
	/**
	 * Clears the range for this config.
	 */
	public void clearRange(){
		this.minValue = Double.MIN_VALUE;
		this.maxValue = Double.MAX_VALUE;
		isRangeUsed = false;
	}
	
	/**
	 * Check if value is in the range. Must min / max must
	 * be set with the setValueRange method. 
	 * Min is inclusive.
	 * max is exclusive.   
	 * @param value a double to check agains the min and max.
	 * @return a true is in range, false if not. 
	 * @throws IllegalArgumentException isRangeUsed is not set to true, use the
	 * setValueRange to prevent this exception.  
	 */
	public boolean isInRangeExclusiveMax(Double value) throws IllegalArgumentException{
		if(isRangeUsed){
			return value >= minValue && value < maxValue;
		}
		throw new IllegalArgumentException("The range is not used: must setValueRange first.");
	}
	
	/**
	 * Check if the country is a valid country for this configuration.
	 * Assumes that no countries in this config then it will return true.
	 * Will return the result of the contains method otherwise.
	 * @param country
	 * @return true if not countries are set or if countries contains
	 * country.  False otherwise.
	 */
	public boolean isValidCountryCode(String country){
		if(countryCodes.size() == 0){
			return true;
		}
		return countryCodes.contains(country);
	}
	
	/**
	 * Removes all countries from the country code set.
	 */
	public void clearCountryCodes(){
		countryCodes.clear();
	}
	/**
	 * Adds a country to the country set.
	 * @param country a String with the country name.
	 */
	public void addCountryCode(String code){
		countryCodes.add(code);
	}
	/**
	 * Checks if the code is a valid code.  If no codes have
	 * been set then it returns true.  Else it returns the 
	 * output from the contains method.
	 * @param code a string representing the code.
	 * @return true no codes set or code is in the code
	 * set.  False otherwise.
	 */
	public boolean isValidIndicatorCode(String code){
		if(indicatorCodes.size() == 0){
			return true;
		}
		return indicatorCodes.contains(code);
	}
	
	/**
	 * Removes all indicator codes.
	 */
	public void clearIndicatorCodes(){
		indicatorCodes.clear();
	}
	/**
	 * Adds an indicator code to the set.  
	 * @param code a string containing the code.
	 */
	public void addIndicatorCode(String code){
		indicatorCodes.add(code);
	}
	
	/**
	 * Load default codes used with probem 1
	 */
	public void loadFemaleCummulativeEducationCodes(){
		
		indicatorCodes.add("SE.PRM.CUAT.FE.ZS");
		indicatorCodes.add("SE.SEC.CUAT.UP.FE.ZS");
		indicatorCodes.add("SE.SEC.CUAT.LO.FE.ZS");
		indicatorCodes.add("SE.SEC.CUAT.PO.FE.ZS");
		indicatorCodes.add("SE.TER.CUAT.ST.FE.ZS");
		indicatorCodes.add("SE.TER.CUAT.BA.FE.ZS");
		indicatorCodes.add("SE.TER.CUAT.MS.FE.ZS");			
	}
	
	/**
	 * Load all years from 1960 to 2016
	 */
	public void loadAllYears(){
		addYearSeries(1960, 2016);
	}
	
	public void addYearSeries(int start, int end){
		for(int i = start; i <= end; i++) {
			years.add(Integer.toString(i));
		}
	}
	
	public void addYear(int year){
		years.add(Integer.toString(year));
	}
	/**
	 * Clears all years from the table
	 */
	public void clearYears(){
		this.years.clear();
	}
	public void loadCummulativeEducationTitleMap(){
		titleMap.put("SE.PRM.CUAT.FE.ZS", "Female-Primary");
		titleMap.put("SE.SEC.CUAT.LO.FE.ZS", "Female-Lower-Secondary");
		titleMap.put("SE.SEC.CUAT.UP.FE.ZS", "Female-Upper-Secondary");
		titleMap.put("SE.SEC.CUAT.PO.FE.ZS", "Female-Post-Secondary");
		titleMap.put("SE.TER.CUAT.ST.FE.ZS", "Female-Short-Cycle-Ternary");
		titleMap.put("SE.TER.CUAT.BA.FE.ZS", "Female-Bachelor's");
		titleMap.put("SE.TER.CUAT.MS.FE.ZS", "Female-Master's");
		titleMap.put("SE.TER.CUAT.DO.FE.ZS", "Female-Doctoral");

		titleMap.put("SE.PRM.CUAT.MA.ZS", "Male-Primary");
		titleMap.put("SE.SEC.CUAT.UP.MA.ZS", "Male-Upper-Secondary");
		titleMap.put("SE.SEC.CUAT.LO.MA.ZS", "Male-Lower-Secondary");
		titleMap.put("SE.SEC.CUAT.PO.MA.ZS", "Male-Post-Secondary");
		titleMap.put("SE.TER.CUAT.ST.MA.ZS", "Male-Short-Cycle-Ternary");
		titleMap.put("SE.TER.CUAT.BA.MA.ZS", "Male-Bachelor's");
		titleMap.put("SE.TER.CUAT.BS.MA.ZS", "Male-Master's");
		titleMap.put("SE.TER.CUAT.DO.MA.ZS", "Male-Doctoral");
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public boolean isRangeUsed() {
		return isRangeUsed;
	}

	public void setRangeUsed(boolean isRangeUsed) {
		this.isRangeUsed = isRangeUsed;
	}

	public double getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}

	public double getMinValue() {
		return minValue;
	}

	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}

	public Set<String> getCountryCodes() {
		return countryCodes;
	}

	public void setCountryCodes(Set<String> countryCodes) {
		this.countryCodes = countryCodes;
	}

	public List<String> getYears() {
		return years;
	}

	public void setYears(List<String> years) {
		this.years = years;
	}

	public Set<String> getIndicatorCodes() {
		return indicatorCodes;
	}

	public void setIndicatorCodes(Set<String> indicatorCodes) {
		this.indicatorCodes = indicatorCodes;
	}

	public Map<String, String> getTitleMap() {
		return titleMap;
	}

	public void setTitleMap(Map<String, String> titleMap) {
		this.titleMap = titleMap;
	}
	
}
