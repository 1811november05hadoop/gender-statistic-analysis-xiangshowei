package com.revature.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FemaleGraduationRateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static final Logger LOGGER = Logger.getLogger(FemaleGraduationRateMapper.class); 
 
	private static final int COUNTRY_NAME_COLUMN = 0;
	private static final int COUNTRY_CODE_COLUMN = 1;
	private static final int INDICATOR_CODE_COLUMN = 3;
	private static final int START_YEAR_COLUMN = 4;
	private static final int END_YEAR_COLUMN = 59;

	//Gross graduation ratio, tertiary, female (%)
	private static final String INDICATOR_CODE = "SE.TER.CMPL.FE.ZS";

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

			for (int i = START_YEAR_COLUMN; i <= END_YEAR_COLUMN; i++) {
				String graduationRateInYearStr = row[i];
				double graduationRateInYear = 0;
				
				if (!graduationRateInYearStr.isEmpty()) {
					graduationRateInYear = Double.parseDouble(graduationRateInYearStr);					
				}
				
				if (graduationRateInYear < 30) 
					/* pads the output with spaces so it reaches 38 characters for displaying purposes
					 * NOTE: a tab(\t) counts as 4 spaces in size 
					 * 
					 * [java.lang.String]	String.format("%-38s", countryName)
					 * 		https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#format%28java.lang.String,%20java.lang.Object...%29
					 * 
					 * ----------copied from official documentation----------
					 * 
					 * Format(ter) String syntax: %[argument_index$][flags][width][.precision]conversion
					 * 
					 * [argument_index$] The optional argument_index is a decimal integer indicating the position of the argument in the argument list. 
					 * 					 The first argument is referenced by "1$", the second by "2$", etc.
					 * 
					 * [flags] The optional flags is a set of characters that modify the output format. The set of valid flags depends on the conversion.
					 * 
					 * [width] The optional width is a non-negative decimal integer indicating the minimum number of characters to be written to the output.
					 * 
					 * [.precision] The optional precision is a non-negative decimal integer usually used to restrict the number of characters. 
					 * 				The specific behavior depends on the conversion.
					 * 
					 * conversion: The required conversion is a character indicating how the argument should be formatted. 
					 * 		      The set of valid conversions for a given argument depends on the argument's data type. 
					 * 
					 * ----------copied from official documentation----------
					 * 
					 * 
					 * 
					 * System.out.printf syntax: %[flags][width][.precision][argsize]typechar 
					 * 		https://sharkysoft.com/archive/printf/docs/javadocs/lava/clib/stdio/doc-files/specification.htm
					 * 
					 * ----------copied from above link----------
					 * 
					 * % indicates that it's a format code  
					 * [flags] optional prefix; "-" flag left justifies(align left) or pads to the right of the String
					 * 
					 * [width] optional "width specifier", if present, indicates the field width, 
					 * 			or the minimum number of characters in the output that the formatted argument will span. 
					 * 				-If the string representation of the value does NOT fill the minimum length, the field will be left-padded with spaces. 
					 * 				-If the converted value exceeds the minimum length, however, the converted result will NOT be truncated.
					 * 
					 * [.precision] optional "precision specifier" may be included in a format specifier to indicate the precision with which to convert the data
					 * 
					 * [argsize]
					 * 
					 * typechar: a single character identifying the conversion type
					 * 
					 * ----------copied from above link----------
					 * 
					 * [org.apache.hadoop]	StringUtils.rightPad(countryName, 38, "") uses more heap space!
					 * 		https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/StringUtils.html#rightPad-java.lang.String-int-
					 */

					context.write(new Text(String.format("%-38s", countryName)), new DoubleWritable(graduationRateInYear));
			}
		}
	}
}