package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GlobalFemaleGraduationRateReducer extends Reducer <Text, Text, Text, Text> {
	
	//number of characters between the first character of first value and first character of subsequent value
	private static final int NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS = 16;
	public static final int NUM_CHARACTERS_UNTIL_FIRST_VALUE = 34;
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		StringBuilder output = new StringBuilder();
		
		for (Text value : values) {
			/* String.format() starts counting characters with respect 
			 * to the string in its second argument while disregarding 
			 * any String concatenations that precede the string; 
			 * the number of characters will be counted starting from 
			 * output.String()'s first character and disregard the left parenthesis  
			 * 
			 * ex: There are 16 characters starting from 
			 * the 1 in 1960 until the fist 1 and 1961 
			 * (1960,2.6648)   (1961,2.86104) 
			 */
			
			/* ##### Pads the output with spaces until it reaches 
			 * the specified amount of characters (for displaying purposes) #####
			 * 
			 * NOTE: a tab(\t) counts as 4 spaces in size 
			 * 
			 * 1) [java.lang.String]	String.format("%-34s", countryName)
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
			 * 2) [org.apache.commons.lang3]	StringUtils.rightPad(countryName, 34, "") uses more heap space!
			 * 		https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/StringUtils.html#rightPad-java.lang.String-int-
			 * 
			 * ##### Printing formatted String to console #####
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
			 */
			output.append(String.format("%-" + NUM_CHARACTERS_BETWEEN_FIRST_CHARACTERS + "s", "(" + value.toString() + ")"));
		}

		context.write(new Text(String.format("%-" + NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", key.toString())), new Text(output.toString()));
	}

}
