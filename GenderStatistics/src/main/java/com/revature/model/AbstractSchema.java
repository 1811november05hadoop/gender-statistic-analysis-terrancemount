package com.revature.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public abstract class AbstractSchema implements Schema{
	private static Logger LOGGER = Logger.getLogger(AbstractSchema.class);
	private Map<String, String> valueMap = new HashMap<>();
	private Map<Integer, Double> yearValues = new HashMap<>();
	public static final int NUMBER_TEXT_COLUMNS = 4;
	
	//reference to the static columns list in the child class.
	private List<String> columns;
	
	//this constructor gets the columns static from the child class.  
	public AbstractSchema() {
		LOGGER.trace("constructor(), calls getColumns from child class.");
		this.columns = getColumns();
	}
	
	@Override
	public void putRow(String row, String separator) {
		LOGGER.trace(String.format("putRow(%10s, %s) **truncated", row, separator));
		
		String[] values = row.split(separator);
		
		String cleanValue;
		
		for(int i = 0; i < NUMBER_TEXT_COLUMNS; i++) {
			cleanValue = values[i].trim().replaceAll("\"", "");
			if(cleanValue.length() > 0) {
				valueMap.put(columns.get(i), cleanValue);
			}
		}
		for(int i = NUMBER_TEXT_COLUMNS; i < values.length; i++){
			cleanValue = values[i].trim().replaceAll("\"", "");
			Integer year;
			Double value;
			try{
				year = Integer.valueOf(columns.get(i));
				value = Double.valueOf(cleanValue);
			} catch (NumberFormatException e){
				continue;
			}
			yearValues.put(year, value);
		}
	}
	
	@Override
	public void putData(String key, String value) {
		LOGGER.trace(String.format("putData(%s, %s)", key, value));
		valueMap.put(key, value);
	}
	@Override
	public void putData(Integer year, Double value){
		LOGGER.trace(String.format("putData(%s, %s)", year, value));
		yearValues.put(year, value);
		
	}
	
	@Override
	public void clearRow() {
		LOGGER.trace("clearRow()");
		valueMap.clear();
		yearValues.clear();
	}
	
	@Override
	public String getValueFromColumnName(String columnName) {
		LOGGER.trace(String.format("getValueFromColumnName(%s)", columnName));
		String output = valueMap.get(columnName);
		
		if(output == null)
			return "";
		
		return output;
	}
	
	@Override
	public Double getValueForYear(int year){
		return yearValues.get(year);
	}
	
	@Override
	public String getValueFromColumnIndex(int columnIndex) throws IndexOutOfBoundsException{
		LOGGER.trace(String.format("getValueFromColumnIndex(%d)", columnIndex));
		if(columnIndex < 0 || columnIndex >= columns.size()) {
			throw new IndexOutOfBoundsException("index: " + columnIndex + " is not defined in the current schema.");
		}
		return getValueFromColumnName(columns.get(columnIndex));
	}
	
	@Override
	public String getColumnName(int columnIndex) {
		LOGGER.trace(String.format("getColumnName(%d)", columnIndex));
		if(columnIndex < 0 || columnIndex >= columns.size()) {
			throw new IndexOutOfBoundsException("index: " + columnIndex + " is not defined in the current schema.");
		}
		return columns.get(columnIndex);
	}

	@Override
	public int getColumnIndex(String columnName) {
		LOGGER.trace(String.format("getColumnIndex(%s)", columnName));
		return columns.indexOf(columnName);
	}

	@Override
	public String[] getAllColumnNames() {
		LOGGER.trace("getAllColumnNames()");
		return columns.toArray(new String[0]);
	}

	@Override
	public String toStringColumnNames() {
		LOGGER.trace("toStringColumnNames()");
		return columns.toString();
	}

	@Override
	public int getNumberOfColumns() {
		LOGGER.trace("getNumberOfColumns()");
		return columns.size();
	}

	@Override
	public boolean isSchemaEmpty() {
		LOGGER.trace("isSchemaEmpty()");
		return columns.isEmpty();
	}

	
	/* ******************************************************
	 * Boiler plate
	 ********************************************************/
	

	public Map<String, String> getValueMap() {
		return valueMap;
	}

	public void setValueMap(Map<String, String> valueMap) {
		this.valueMap = valueMap;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((valueMap == null) ? 0 : valueMap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractSchema other = (AbstractSchema) obj;
		if (valueMap == null) {
			if (other.valueMap != null)
				return false;
		} else if (!valueMap.equals(other.valueMap))
			return false;
		return true;
	}

	@Override
	public String toString() {
		String inOrderTable = "[";
		String columnName;
		for(int i = 0; i < columns.size(); i++) {
			if(i!= 0) {
				inOrderTable += ", ";
			}
			columnName = columns.get(i);
			inOrderTable += "{" + columnName + " = " + valueMap.get(columnName) + "}";
		}
		
		inOrderTable += "]";
		
		return "AbstractSchema [valueMap=" + inOrderTable + "]";
	}
	
	
}
