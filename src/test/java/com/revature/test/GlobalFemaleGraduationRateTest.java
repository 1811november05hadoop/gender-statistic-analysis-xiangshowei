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

import com.revature.map.GlobalFemaleGraduationRateMapper;
import com.revature.reduce.GlobalFemaleGraduationRateReducer;

public class GlobalFemaleGraduationRateTest {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
	private String input;
	
	@Before
	public void setUp() {
		GlobalFemaleGraduationRateMapper fgrMapper = new GlobalFemaleGraduationRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(fgrMapper);

		GlobalFemaleGraduationRateReducer fgrReducer = new GlobalFemaleGraduationRateReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(fgrReducer);
		
		mrDriver = new MapReduceDriver<>();
		mrDriver.setMapper(fgrMapper);
		mrDriver.setReducer(fgrReducer);
	}
	
	@Test
	public void testGlobalFemaleGraduationRateMapper() {
		input = simulateInput("Afghanistan","AFG","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","1.40144","","","","0.8031","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","");
		
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("Afghanistan"), new Text("1975,1.40144"));
		mapDriver.withOutput(new Text("Afghanistan"), new Text("1979,0.8031"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testGlobalFemaleGraduationRateReducer() {
		List<Text> values = new ArrayList<>();
		
		values.add(new Text("1975,1.40144"));
		values.add(new Text("1979,0.8031"));
		
		String formattedKey = String.format("%-" + GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Afghanistan");
		
		reduceDriver.withInput(new Text(formattedKey), values);
		reduceDriver.withOutput(new Text(formattedKey), new Text("(1975,1.40144)  (1979,0.8031)   "));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testGlobalFemaleGraduationRateMapReduce() {
		input = simulateInput("Afghanistan","AFG","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","1.40144","","","","0.8031","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","");
		
		mrDriver.withInput(new LongWritable(1), new Text(input));
		String formattedKey = String.format("%-" + GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Afghanistan");
		mrDriver.withOutput(new Text(formattedKey), new Text("(1975,1.40144)  (1979,0.8031)   "));
		
		mrDriver.runTest(); 
	}
	
	/**
	 * Appends double quotes and commas to input string 
	 * for each argument to simulate the input received by 
	 * mapper and reducer
	 */
	public static String simulateInput(String... input) {
		StringBuilder sb = new StringBuilder(input.length);
		
		for (int i = 0; i < input.length; i++) {
			sb.append("\"" + input[i] + "\",");
		}
		
		return sb.toString();
	}
}
