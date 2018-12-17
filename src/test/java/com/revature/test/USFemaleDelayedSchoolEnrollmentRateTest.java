package com.revature.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import com.revature.map.USFemaleDelayedSchoolEnrollmentRateMapper;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateCombiner;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateReducer;

public class USFemaleDelayedSchoolEnrollmentRateTest {
	
	private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
	private ReduceDriver<IntWritable, Text, IntWritable, Text> combineDriver;
	private ReduceDriver<IntWritable, Text, IntWritable, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, DoubleWritable> mrDriver;
	
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
	

}
