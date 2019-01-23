package com.revature.map;

import static com.revature.map.GlobalFemaleGraduationRateMapper.COUNTRY_NAME_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.COUNTRY_CODE_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.INDICATOR_CODE_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.END_YEAR_COLUMN;
import static com.revature.map.GlobalFemaleGraduationRateMapper.CONGLOMERATE_COUNTRY_CODES;
import static com.revature.reduce.GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmploymentRateChangeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private static final int START_YEAR_COLUMN = END_YEAR_COLUMN - 14;
	//Employment to population ratio, 15+, male (%) (modeled ILO estimate)
	private static final String INDICATOR_CODE = "SL.EMP.TOTL.SP.MA.ZS";
	
	private static final int NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS = 8;
	private static final int DECIMAL_PLACES = 3;
	
//	@Override
//	public void setup(Context context) throws IOException, InterruptedException {
//		
//		Text formattedHeaderKey = new Text(String.format("%-" + NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Country"));
//		Text formattedHeaderValue = new Text("2000-01 *  2001-02 *  2002-03 *  2003-04 *  2004-05 *  2005-06 *  2006-07 *  "
//				+ "2007-08 *  2008-09 *  2009-10 *  2010-11 *  2011-12 *  2012-13 *  2013-14 *  2014-15 *  2015-16 *  ");
//		
//		context.write(formattedHeaderKey, formattedHeaderValue);
//	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String inputSplit = value.toString();
		String[] row = inputSplit.split("\",\"");

		String countryCode = row[COUNTRY_CODE_COLUMN];		
		String indicatorCode = row[INDICATOR_CODE_COLUMN];

		boolean relevantData = false;

		if(!CONGLOMERATE_COUNTRY_CODES.contains(countryCode)) {
			if(indicatorCode.equals(INDICATOR_CODE)) {
				relevantData = true;
			}
		}
		
		if(relevantData) {
			String countryName = row[COUNTRY_NAME_COLUMN].replaceFirst("\"", "");
			StringBuilder outputValue = new StringBuilder();
			
			for (int i = START_YEAR_COLUMN; i <= END_YEAR_COLUMN; i++) {
				String unemploymentRateStartYear = row[i - 1];
				String unemploymentRateEndYear = row[i];
				double unemploymentRateDelta;
				
				if (!unemploymentRateStartYear.isEmpty()) {
					if(!unemploymentRateEndYear.isEmpty()) {
						
						unemploymentRateDelta = Double.parseDouble(unemploymentRateEndYear) 
								- Double.parseDouble(unemploymentRateStartYear);
						
						//add + sign to output if the result is 0 or higher 
						if (unemploymentRateDelta >= 0 ) {
							/* employment rate between subsequent years are separated by 
							 * the specified number of characters apart;
							 * 
							 * employment rates themselves are limited to
							 * the specified number of decimal places
							 */
							outputValue.append("+" + String.format("%-" + (NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS - 1) + "s", 
									String.format("%."+ DECIMAL_PLACES+ "f%%", unemploymentRateDelta)) + " *  ");
						}
						
						else {
							outputValue.append(String.format("%-" + NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS + "s", 
								String.format("%."+ DECIMAL_PLACES+ "f%%", unemploymentRateDelta)) + " *  ");
						}
					}
				}
			}
			
			Text formattedKey = new Text(String.format("%-" + NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", countryName.toString()));
					
			context.write(formattedKey, new Text(outputValue.toString()));		
		}
	}
}
