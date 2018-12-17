package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.USFemaleDelayedSchoolEnrollmentRateMapper;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateCombiner;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateReducer;

public class USFemaleDelayedSchoolEnrollmentRateTest {
	
	private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
	private ReduceDriver<IntWritable, Text, IntWritable, Text> combineDriver;
	private ReduceDriver<IntWritable, Text, IntWritable, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, DoubleWritable> mrDriver;
	private String input;
	
	@Before
	public void setUp() {
		USFemaleDelayedSchoolEnrollmentRateMapper usFDSERMapper = new USFemaleDelayedSchoolEnrollmentRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(usFDSERMapper);
		
		USFemaleDelayedSchoolEnrollmentRateCombiner usFDSERCombiner = new USFemaleDelayedSchoolEnrollmentRateCombiner();
		combineDriver = new ReduceDriver<>();
		combineDriver.setReducer(usFDSERCombiner);
		
		USFemaleDelayedSchoolEnrollmentRateReducer usFDSERReducer = new USFemaleDelayedSchoolEnrollmentRateReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(usFDSERReducer);
		
		mrDriver = new MapReduceDriver<>();
		mrDriver.setMapper(usFDSERMapper);
		mrDriver.setCombiner(usFDSERCombiner);
		mrDriver.setReducer(usFDSERReducer);
	}
	
//	@Test
//	public void testMapper() {
//		input = GlobalFemaleGraduationRateTest.simulateInput("United States","USA","School enrollment, secondary, female (% gross)","SE.SEC.ENRR.FE","","","","","","","","","","","","","60.67222","","","","","","85.3929","","","91.41668","94.67477","94.75381","96.46224","95.0773","96.60813","96.64305","","","91.47174","92.25663","","95.98984","96.81125","96.40682","95.25981","","96.92163","","93.88283","94.77221","93.0587","95.3905","96.89634","97.06396","95.63982","96.82829","96.10534","96.43099","95.16566","95.25471","95.58667","96.37354","98.47282","","","");
//		String input2 = GlobalFemaleGraduationRateTest.simulateInput("United States","USA","School enrollment, secondary, female (% net)","SE.SEC.NENR.FE","","","","","","","","","","","","","","","","","","","","","","","","","","","","90.75925","","","","","","88.65025","89.46821","89.96469","88.23913","","","","87.40828","88.33398","86.02794","90.2675","91.94563","91.7294","91.08409","91.20605","91.10048","90.76806","88.95475","89.57585","89.46749","90.15375","91.98203","","","");
//		
//		mapDriver.withInput(new LongWritable(1), new Text(input));
//		mapDriver.withInput(new LongWritable(2), new Text(input2));
//		
//		mapDriver.withOutput(new IntWritable(2010), new Text("g95.16566"));
//		mapDriver.withOutput(new IntWritable(2010), new Text("n88.95475"));
//		mapDriver.withOutput(new IntWritable(2011), new Text("g95.25471"));
//		mapDriver.withOutput(new IntWritable(2011), new Text("n89.57585"));
//		mapDriver.withOutput(new IntWritable(2012), new Text("g95.58667"));
//		mapDriver.withOutput(new IntWritable(2012), new Text("n89.46749"));
//		mapDriver.withOutput(new IntWritable(2013), new Text("g96.37354"));
//		mapDriver.withOutput(new IntWritable(2013), new Text("n90.15375"));
//		mapDriver.withOutput(new IntWritable(2014), new Text("g98.47282"));
//		mapDriver.withOutput(new IntWritable(2014), new Text("n91.98203"));		
//		
//		mapDriver.runTest();
//	}
	
//	@Test
//	public void testCombiner() {
//		List<Text> values = new ArrayList<>();
//		values.add(new Text("95.16566:88.95475"));
//		
//		combineDriver.withInput(new IntWritable(2010), values);
//		
//		combineDriver.runTest();
//	}
	
	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("95.16566:88.95475"));
		
		reduceDriver.withInput(new IntWritable(2010), values);
		reduceDriver.withOutput(new IntWritable(2010), new DoubleWritable(6.210909999999998));
		
		reduceDriver.runTest();
	}
	
//	@Test
//	public void testMapReduce(){
//		input = GlobalFemaleGraduationRateTest.simulateInput("United States","USA","School enrollment, secondary, female (% gross)","SE.SEC.ENRR.FE","","","","","","","","","","","","","60.67222","","","","","","85.3929","","","91.41668","94.67477","94.75381","96.46224","95.0773","96.60813","96.64305","","","91.47174","92.25663","","95.98984","96.81125","96.40682","95.25981","","96.92163","","93.88283","94.77221","93.0587","95.3905","96.89634","97.06396","95.63982","96.82829","96.10534","96.43099","95.16566","95.25471","95.58667","96.37354","98.47282","","","");
//		
//		mrDriver.withInput(new LongWritable(1), new Text(input));
//		mrDriver.withOutput(new IntWritable(2010), new DoubleWritable(6.210909999999998));
//		
//		mrDriver.runTest();
//	}
}
