package com.revature.configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class MapConfiguration {
	private static Logger LOGGER = Logger.getLogger(MapConfiguration.class);
	public static final int MIN_YEAR = 1960;
	public static final int MAX_YEAR = 2016;
	public static final int NUM_YEARS = MAX_YEAR - MIN_YEAR;
	public static final String COUNTRY_CODE = "COUNTRY_CODE";
	public static final String COUNTRY_NAME = "COUNTRY_NAME";
	public static final String INDICATOR_NAME = "INDICATOR_NAME";
	public static final String INDICATOR_CODE = "INDICATOR_CODE";

	Set<String> indicatorCodes = new HashSet<>();
	Set<String> countryCodes = new HashSet<>();
	Map<String, String> heads = new HashMap<>();
	Map<Integer, Double> yearValues = new TreeMap<>();


	public void addRow(String row){
		String[] attributes = row.split("\",\"");
		heads.put(COUNTRY_NAME, attributes[0].replaceAll("\"", ""));
		heads.put(COUNTRY_CODE, attributes[1].replaceAll("\"", ""));
		heads.put(INDICATOR_NAME, attributes[2].replaceAll("\"", ""));
		heads.put(INDICATOR_CODE, attributes[3].replaceAll("\"", ""));

		yearValues.clear();
		for(int i = 0; i <= NUM_YEARS; i++){
			try{
				yearValues.put(i + MIN_YEAR, Double.valueOf(attributes[i + 4].replaceAll("\"", "")));
			}catch (NumberFormatException e){}
		}
	}
	public String getCountryName(){
		return heads.get(COUNTRY_NAME);
	}
	public String getCountryCode(){
		return heads.get(COUNTRY_CODE);
	}
	public String getIndicatorName(){
		return heads.get(INDICATOR_NAME);
	}
	public String getIndicatorCode(){
		return heads.get(INDICATOR_CODE);
	}


	public void addCountryCodes(String... codes){
		for(String code: codes){
			this.countryCodes.add(code);
		}
	}
	public void addCountryCodeString(String codes){
		addIndicatorCodeString(codes, ",");
	}

	public void addCountryCodeString(String codes, String separator){
		String[] splitCodes = codes.split(separator);
		for(String code: splitCodes){
			this.countryCodes.add(code);
		}
	}

	public boolean isCountryCodeValidInRow(){
		if(countryCodes.isEmpty())
			return true;
		return countryCodes.contains(heads.get(COUNTRY_CODE));
	}

	public void addIndicatorCodes(String... codes){
		for(String code: codes){
			this.indicatorCodes.add(code);
		}
	}
	public void addIndicatorCodeString(String codes){
		addIndicatorCodeString(codes, ",");
	}

	public void addIndicatorCodeString(String codes, String separator){
		String[] splitCodes = codes.split(separator);
		for(String code: splitCodes){
			this.indicatorCodes.add(code);
		}
	}
	public boolean isIndicatorCodeValidInRow(){
		if(indicatorCodes.isEmpty()){
			return true;
		}
		return indicatorCodes.contains(heads.get(INDICATOR_CODE));
	}


	public boolean isValuesEmpty(){
		return yearValues.isEmpty();
	}

	public Map<Integer, Double> getValues(){
		return yearValues;
	}

	public String getShortIndicatorTitleFromRow(){
		String[] parts = heads.get(INDICATOR_CODE).split("[.]");
		String output = "";

		if(parts[parts.length - 2].equals("FE")){
			output = "Female";
		}else{
			output = "Male";
		}

		switch (parts[0]){
		case "SE":
			output += "-Education";
		}

		switch (parts[1]){
		case "PRM":
			output += "-Primary";
			break;
		case "NED":
			output += "-NoSchooling";
			break;
		case "IPR":
			output += "-NoSchooling";
			break;
		case "SEC":
			output += "-Secondary";
			switch (parts[3]){
			case "LO":
				output+= "-Lower";
				break;
			case "UP":
				output+= "-Upper";
				break;
			case "PO":
				output+= "-Post";
				break;
			}
			break;
		case "TER":
			output += "-Ternary";
			switch (parts[3]){
			case "ST":
				output+= "-Short-cycle Ternary";
				break;
			case "BA":
				output+= "-Bachelor's";
				break;
			case "MS":
				output+= "-Master's";
				break;
			case "DO":
				output+= "-Docotral";
				break;
			}
			break;
		}


		//		
		//		switch (parts[2]){
		//		case "CUAT":
		//			output += " Highest Level";
		//			break;
		//		case "HIAT":
		//			output += " At least";
		//			break;
		//		}	



		return output;
	}
	@Override
	public String toString() {
		return "MapConfiguration [indicatorCodes=" + indicatorCodes
				+ ", countryCodes=" + countryCodes + ", heads=" + heads
				+ ", yearValues=" + yearValues + "]";
	}




}	
