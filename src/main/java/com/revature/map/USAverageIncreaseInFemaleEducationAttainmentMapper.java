package com.revature.map;

import static com.revature.map.GlobalFemaleGraduationRateMapper.END_YEAR_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.COUNTRY_CODE_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.INDICATOR_CODE_COLUMN;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * List the average increase in female education in the U.S. from the year 2000.
 *
 */
public class USAverageIncreaseInFemaleEducationAttainmentMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

	private static final int START_YEAR_COLUMN = END_YEAR_COLUMN - 15;

	public static final String COUNTRY_CODE = "USA";
	//Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)
	private static final String INDICATOR_CODE = "SE.SEC.CUAT.LO.FE.ZS";

	@Override
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
			for (int i = START_YEAR_COLUMN + 1; i <= END_YEAR_COLUMN; i++) {
				String educationAttainmentRateStartYearStr = row[i - 1];
				String educationAttainmentRateEndYearStr = row[i];
				double educationAttainmentRateDelta;

				//only consider as a data point if there are data for 2 consecutive years; 
				//can't calculate change for each year with only one year's worth of data
				if(!educationAttainmentRateStartYearStr.isEmpty()) {
					if(!educationAttainmentRateEndYearStr.isEmpty()) {
						educationAttainmentRateDelta = Double.parseDouble(educationAttainmentRateEndYearStr) 
								- Double.parseDouble(educationAttainmentRateStartYearStr);
						
						context.write(NullWritable.get(), new DoubleWritable(educationAttainmentRateDelta));
					}
				}	
			}			
		}
	}
}
