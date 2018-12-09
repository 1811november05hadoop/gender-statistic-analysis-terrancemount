package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.revature.map.GenderDataByCountryWithYearAndPercentageMapper;
import com.revature.model.GenderDataSchemaImpl;
import com.revature.model.Schema;

public class TestDriver {

	public static void main(String[] args) throws IOException, InterruptedException {
		Schema gd = new GenderDataSchemaImpl();
		System.out.println(gd.toStringColumnNames());
		
		gd.addRow("\"Arab World\",\"ARB\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"29.08\",\"20.001\"", "\",\"");
		System.out.println(gd.toString());
		
		GenderDataByCountryWithYearAndPercentageMapper mapper = new GenderDataByCountryWithYearAndPercentageMapper();
		
		System.out.println(mapper.toString());
		
		
		mapper.map(new LongWritable(1),
				new Text("\"Arab World\",\"ARB\",\"Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20\""),
				null);
	}
}
