package com.revature.model;

import java.util.List;

public interface Schema {
	
	
	/**
	 * Returns a new instance of the concrete class to
	 * be used and manipulated without concerns for
	 * overriding the other instances in different threads.
	 * 
	 * Want to set a static varable to Schema and use this
	 * method to get a new instance of the schema.
	 * 
	 * @return a new instance of the concrete class that implements schema.
	 */
	Schema getNewSchema();
	
	/**
	 * Returns the column name for the given index.
	 * @param columnIndex an integer zero based index
	 * @return a string for the column name.
	 * @throws IndexOutOfBoundsException if the index is out of bounds for the array.  
	 */
	String getColumnName(int columnIndex) throws IndexOutOfBoundsException;
	
	/**
	 * Returns the column index for the given name.
	 * @param columnName a string for the column name.
	 * @return an integer representing the index or a -1 if the index was not found.
	 */
	int getColumnIndex(String columnName);

	/**
	 * Returns the number of columns for the schema
	 * @return an integer for the number of columns.
	 */
	int getNumberOfColumns();
	
	/**
	 * Returns true if no columns have been set and
	 * false if at least one column was set.
	 * @return a boolean if at least one column was set.  
	 */
	boolean isSchemaEmpty();
	
	/**
	 * Returns an array of ordered column names.
	 * @return an array of string for the column names.
	 */
	String[] getAllColumnNames();
	
	/**
	 * Returns a comma separated list of the column names.
	 * @return a string with comma separated column names.
	 */
	String toStringColumnNames();
	
	/**
	 * Add the row value from the input split of the mapper.  This function will
	 * place all the row values into a map for later retrieval.
	 * @param row a string containing all the values for that row.
	 * @param separator is a string for the separator between the rows.  
	 */
	void addRow(String row, String separator);
	
	/**
	 * Puts the key/value pair into a map to be used when
	 * generating mapper output. Put will override any 
	 * previous values so becareful that important data is 
	 * not lost. No duplicate keys allowed.
	 * 
	 * @param key a string key value for the map.
	 * @param value a string for the value.
	 */
	void putData(String key, String value);
	
	/**
	 * Clear the table map of any inputed rows. Useful for when
	 * passing a schema to a static variable on a mapper.   
	 */
	void clearRow();
	
	/**
	 * Returns the string value for named column.
	 * @param columnName a string for the column name to return the value for.
	 * @return a string of the value
	 */
	String getValueFromColumnName(String columnName);
	
	/**
	 * Returns the string value for indexed column.
	 * @param columnName a string for.
	 * @return a string of the value
	 * @throws IndexOutOfBoundsException if the index is out of bounds for the schema
	 */
	String getValueFromColumnIndex(int columnIndex) throws IndexOutOfBoundsException;
	
	/**
	 * Returns the static columns list for the type of schema.
	 * @return a list of string column names.
	 */
	List<String> getColumns();
}
