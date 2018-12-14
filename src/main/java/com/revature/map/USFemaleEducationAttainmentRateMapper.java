package com.revature.map;

import static com.revature.map.GlobalFemaleGraduationRateMapper.*;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USFemaleEducationAttainmentRateMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	private static final int START_YEAR_COLUMN = END_YEAR_COLUMN - 15;

	public static final String COUNTRY_CODE = "USA";
	//Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)
	private static final String INDICATOR_CODE = "SE.SEC.CUAT.LO.FE.ZS";

	public static final int START_YEAR = 2000;

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String inputSplit = value.toString();

		String[] row = inputSplit.split("\",\"");

		String countryCode = row[COUNTRY_CODE_COLUMN];		
		String indicatorCode = row[INDICATOR_CODE_COLUMN];

		boolean relevantData = false;

		if(countryCode.equals(COUNTRY_CODE)){
			if(indicatorCode.equals(INDICATOR_CODE)) {
				relevantData = true;
			}
		}

		if(relevantData) {
			int year = START_YEAR;

			for (int i = START_YEAR_COLUMN; i <= END_YEAR_COLUMN; i++) {
				String educationAttainmentRateStr = row[i];
				double educationAttainmentRate;

				if(!educationAttainmentRateStr.isEmpty()) {
					educationAttainmentRate = Double.parseDouble(educationAttainmentRateStr);
					context.write(new IntWritable(year), new DoubleWritable(educationAttainmentRate));
					
				}
					
				year++;			
			}			
		}
	}
	
	//TODO: overwrite run() method to stop processing after finding the indicator code of the country
}
