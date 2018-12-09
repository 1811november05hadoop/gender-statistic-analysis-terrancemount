package com.revature.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Table that stores the schema of the gender data table.
 * @author Terrance Mount
 *
 */
public class GenderDataSchemaImpl extends AbstractSchema{
	private static Logger LOGGER = Logger.getLogger(GenderDataSchemaImpl.class);
	private static final List<String> columns = new ArrayList<>();
	
	//set up the static schema for the gender data table
	static {
		LOGGER.trace("GenderDataSchemaImpl initalizing columns.");
		columns.add("COUNTRY_NAME");
		columns.add("COUNTRY_CODE");
		columns.add("INDICATOR_NAME");
		columns.add("INDICATOR_CODE");

		//column headings in the table is in years
		for(int i = 1960; i <= 2016; i++) {
			columns.add(Integer.toString(i));
		}
	}
	
	@Override
	public List<String> getColumns() {
		LOGGER.trace("getColumns()");
		return columns;
	}

	@Override
	public Schema getNewSchema() {
		LOGGER.trace("getNewSchema()");
		return new GenderDataSchemaImpl();
	}
	
	
}