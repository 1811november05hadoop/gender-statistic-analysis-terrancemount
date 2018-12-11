package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.revature.config.MapConfig;
import com.revature.map.CountryFemGradMapper;
import com.revature.model.GenderDataSchemaImpl;

public class TestDriver {
	public static void main(String[] args) throws IOException, InterruptedException {
		CountryFemGradMapper mapper = new CountryFemGradMapper();
		MapConfig config = new MapConfig();
		config.loadAllYears();
		config.setValueRange(0.0, 30.0);
		config.loadCummulativeEducationTitleMap();
		config.loadFemaleCummulativeEducationCodes();
		config.setSchema(new GenderDataSchemaImpl());
		CountryFemGradMapper.config = config;
		
		Text VALID_MAPPER_INPUT_1 = new Text("\"Arab World\",\"ARB\",\"Educational attainment, "
				+ "at least completed upper secondary, population 25+, "
				+ "female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"50\",\"16\",\"20\"");
		
		mapper.map(new LongWritable(1), VALID_MAPPER_INPUT_1, null);
		
		
	}
}
