package com.revature;

import com.revature.configuration.MapConfiguration;

public class MapUtilTest {

	public static void main(String[] args) {
		MapConfiguration map = new MapConfiguration();
		
		map.addRow("\"Arab World\",\"ARB\",\"Educational attainment, "
				+ "at least completed upper secondary, population 25+, "
				+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.MA.ZS\""
				+ ",\"25\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"50\",\"16\",\"20\"");
		System.out.println(map);
		System.out.println(map.getShortIndicatorTitleFromRow());
	}
	
}
