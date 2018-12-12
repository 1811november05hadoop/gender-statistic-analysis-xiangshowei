package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleGraduationRateMapper;
import com.revature.reduce.FemaleGraduationRateReducer;

public class FemaleGraduationRateTest {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		FemaleGraduationRateMapper fgrMapper = new FemaleGraduationRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(fgrMapper);

		FemaleGraduationRateReducer fgrReducer = new FemaleGraduationRateReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(fgrReducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(fgrMapper);
		mapReduceDriver.setReducer(fgrReducer);
	}
	
	@Test
	public void testFemaleGraduationRateMapper() {
		mapDriver.withInput(new LongWritable(1), 
				new Text(escapeCharacterInInput("Afghanistan","AFG","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","1.40144","","","","0.8031","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","")));
		
		mapDriver.withOutput(new Text("Afghanistan"), new Text("1975,1.40144"));
		mapDriver.withOutput(new Text("Afghanistan"), new Text("1979,0.8031"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testFemaleGraduationRateReducer() {
		List<Text> values = new ArrayList<>();
		
		values.add(new Text("1975,1.40144"));
		values.add(new Text("1979,0.8031"));
		
		String formattedKey = String.format("%-" + FemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Afghanistan");
		
		reduceDriver.withInput(new Text(formattedKey), values);
		reduceDriver.withOutput(new Text(formattedKey), new Text("(1975,1.40144)  (1979,0.8031)   "));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testFemaleGraduationRateMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text(escapeCharacterInInput("Afghanistan","AFG","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","1.40144","","","","0.8031","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","")));
		
		String formattedKey = String.format("%-" + FemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Afghanistan");
		mapReduceDriver.withOutput(new Text(formattedKey), new Text("(1975,1.40144)  (1979,0.8031)   "));
		
		mapReduceDriver.runTest(); 
	}
	
	/**
	 * Appends double quotes and commas to input string 
	 * for each argument to simulate the input received by 
	 * mapper and reducer
	 */
	private static String escapeCharacterInInput(String... input) {
		StringBuilder sb = new StringBuilder(input.length);
		
		for (int i = 0; i < input.length; i++) {
			sb.append("\"" + input[i] + "\",");
		}
		
		return sb.toString();
	}
}
