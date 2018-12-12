package com.revature.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FemaleGraduationRateMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Logger LOGGER = Logger.getLogger(FemaleGraduationRateMapper.class); 

	private static final int COUNTRY_NAME_COLUMN = 0;
	private static final int COUNTRY_CODE_COLUMN = 1;
	private static final int INDICATOR_CODE_COLUMN = 3;
	private static final int START_YEAR_COLUMN = 4;
	private static final int END_YEAR_COLUMN = 59;
	//Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)
	private static final String INDICATOR_CODE = "SE.SEC.CUAT.LO.FE.ZS";
	private static final int THRESHOLD = 30;
	private static final int START_YEAR = 1960;
	
	private static final HashSet<String> conglomerateCountryCodes = 
			new HashSet<>(Arrays.asList("Country Code", 
					"ARB", "CSS", "CEB", "EAR", "EAS",
					"EAP", "TEA", "EMU", "ECS", "ECA", 
					"TEC", "EUU", "FCS", "HPC", "HIC", 
					"IBD", "IBT", "IDB", "IDX", "IDA", 
					"LTE", "LCN", "LAC", "TLA", "LDC", 
					"LMY", "LIC", "LMC", "MEA", "MNA", 
					"TMN", "MIC", "NAC", "OED", "OSS", 
					"PSS", "PST", "PRE", "SST", "SAS", 
					"TSA", "SSF", "SSA", "TSS", "UMC", "WLD"));

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String inputSplit = value.toString();
		/*
		 *row[0] = Country Name
		 *row[1] = Country Code
		 *row[2] = Indicator Name
		 *row[3] = Indicator Code
		 *row[4] = 1960
		 *	.
		 *	.
		 *	.
		 *row[59] = 2016
		 *row[60] = ",
		 */

		/* Some rows have columns that contain commas 
		 * so only the commas that separate the columns are relevant. 
		 * These relevant commas happen to be surrounded
		 * by double quotes in the data
		 */
		String[] row = inputSplit.split("\",\"");

		String countryCode = row[COUNTRY_CODE_COLUMN];		
		String indicatorCode = row[INDICATOR_CODE_COLUMN];

		boolean relevantData = false;

		if(!conglomerateCountryCodes.contains(countryCode)) {
			if(indicatorCode.equals(INDICATOR_CODE)) {
				relevantData = true;
			}
		}

		if(relevantData) {
			// Country Name is the first column in the data so it doesn't match the regular expression
			String countryName = row[COUNTRY_NAME_COLUMN].replaceFirst("\"", "");
			int year = START_YEAR - 1;

			for (int i = START_YEAR_COLUMN; i <= END_YEAR_COLUMN; i++) {
				String graduationRateInYearStr = row[i];
				double graduationRateInYear;
				year++;

				if (!graduationRateInYearStr.isEmpty()) {
					graduationRateInYear = Double.parseDouble(graduationRateInYearStr);	
					
					if (graduationRateInYear < THRESHOLD) {
						context.write(new Text(countryName), new Text(year + "," + graduationRateInYear));
					}
				}
			}
		}
	}
}