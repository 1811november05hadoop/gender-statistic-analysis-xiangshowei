package com.revature.map;

import static com.revature.map.GlobalFemaleGraduationRateMapper.END_YEAR_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.COUNTRY_CODE_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.INDICATOR_CODE_COLUMN;
import static com.revature.map.USAverageIncreaseInFemaleEducationAttainmentMapper.COUNTRY_CODE;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USFemaleDelayedSchoolEnrollmentRateMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	private static final int START_YEAR_COLUMN = END_YEAR_COLUMN - 5;
	private static final String FEMALE_GROSS_SCHOOL_ENROLLMENT_INDICATOR_CODE = "SE.SEC.ENRR.FE";
	private static final String FEMALE_NET_SCHOOL_ENROLLMENT_INDICATOR_CODE = "SE.SEC.NENR.FE";
	private static final int START_YEAR = 2010;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String inputSplit = value.toString();
		String[] row = inputSplit.split("\",\"");
		String countryCode = row[COUNTRY_CODE_COLUMN];
		String indicatorCode = row[INDICATOR_CODE_COLUMN];

		boolean relevantData = false;

		if(countryCode.equals(COUNTRY_CODE)){
			if(indicatorCode.equals(FEMALE_GROSS_SCHOOL_ENROLLMENT_INDICATOR_CODE) || 
					indicatorCode.equals(FEMALE_NET_SCHOOL_ENROLLMENT_INDICATOR_CODE)) {
				relevantData = true;
			}
		}

		if(relevantData) {
			int year = START_YEAR;

			for (int i = START_YEAR_COLUMN; i <= END_YEAR_COLUMN; i++) {
				
				String femaleGrossSchoolEnrollmentRateInYearStr;
				String femaleNetSchoolEnrollmentRateInYearStr;
				StringBuilder schoolEnrollmentRateInYear = new StringBuilder();

				if(indicatorCode.equals(FEMALE_GROSS_SCHOOL_ENROLLMENT_INDICATOR_CODE)) {
					femaleGrossSchoolEnrollmentRateInYearStr = row[i];
					
					if(!femaleGrossSchoolEnrollmentRateInYearStr.isEmpty()) {
						schoolEnrollmentRateInYear.append('g' + femaleGrossSchoolEnrollmentRateInYearStr );
						context.write(new IntWritable(year), new Text(schoolEnrollmentRateInYear.toString()));
					}
				}

				else if(indicatorCode.equals(FEMALE_NET_SCHOOL_ENROLLMENT_INDICATOR_CODE)) {
					femaleNetSchoolEnrollmentRateInYearStr = row[i];
					
					if(!femaleNetSchoolEnrollmentRateInYearStr.isEmpty()) {
						schoolEnrollmentRateInYear.append('n' + femaleNetSchoolEnrollmentRateInYearStr);
						context.write(new IntWritable(year), new Text(schoolEnrollmentRateInYear.toString()));
					}
				}

				year++;
			}			
		}
	}
}
