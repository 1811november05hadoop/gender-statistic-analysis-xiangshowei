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

public class FemaleEmploymentRateChangeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private static final int START_YEAR_COLUMN = END_YEAR_COLUMN - 14;
	//Employment to population ratio, 15+, female (%) (modeled ILO estimate)
	private static final String INDICATOR_CODE = "SL.EMP.TOTL.SP.FE.ZS";
	
	private static final int NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS = 8;
	private static final int DECIMAL_PLACES = 3;
	
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
						
						if (unemploymentRateDelta >= 0 ) {
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
