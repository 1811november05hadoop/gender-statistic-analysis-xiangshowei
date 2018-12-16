package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.USAverageIncreaseInFemaleEducationAttainmentMapper;
import com.revature.reduce.USAverageIncreaseInFemaleEducationAttainmentReducer;

public class USAverageIncreaseInFemaleEducationAttainmentTest {
	
	private MapDriver<LongWritable, Text, NullWritable, DoubleWritable> mapDriver;
	private ReduceDriver<NullWritable, DoubleWritable, NullWritable, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, NullWritable, DoubleWritable, NullWritable, DoubleWritable > mrDriver;
	private String input;

	@Before
	public void setUp() {
		USAverageIncreaseInFemaleEducationAttainmentMapper usAIIFEAMapper = new USAverageIncreaseInFemaleEducationAttainmentMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(usAIIFEAMapper);
		
		USAverageIncreaseInFemaleEducationAttainmentReducer usAIIFEAReducer = new USAverageIncreaseInFemaleEducationAttainmentReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(usAIIFEAReducer);
		
		mrDriver = new MapReduceDriver<>();
		mrDriver.setMapper(usAIIFEAMapper);
		mrDriver.setReducer(usAIIFEAReducer);
	}
	
	@Test
	public void testUSAverageIncreaseInFemaleEducationAttainmentMapper() {
		input = GlobalFemaleGraduationRateTest.simulateInput("United States","USA","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","","","","","","93.01367","","","","","","","","","","","","","","","","","","","","","","","","93.89355","93.88051","94.46581","","94.70972","94.71217","94.93398","95.04402","95.14301","95.39159","95.42985","95.47838","","");
		
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(-0.013040000000003715));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.5853000000000037));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.0024499999999960664));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.22181000000000495));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.11003999999999792));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.09899000000000058));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.2485799999999898));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.038260000000008176));
		mapDriver.withOutput(NullWritable.get(), new DoubleWritable(0.04852999999999952));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testUSAverageIncreaseInFemaleEducationAttainmentReducer() {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(-3.2));
		values.add(new DoubleWritable(5.5));
		values.add(new DoubleWritable(4.5));
		
		reduceDriver.withInput(NullWritable.get(), values);
		reduceDriver.withOutput(NullWritable.get(), new DoubleWritable(5));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testUSAverageIncreaseInFemaleEducationAttainmentMapReduceDriver() {
		input = GlobalFemaleGraduationRateTest.simulateInput("United States","USA","Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)","SE.SEC.CUAT.LO.FE.ZS","","","","","","","","","","","","","","","","","","","","","93.01367","","","","","","","","","","","","","","","","","","","","","","","","93.89355","93.88051","94.46581","","94.70972","94.71217","94.93398","95.04402","95.14301","95.39159","95.42985","95.47838","","");
		
		mrDriver.withInput(new LongWritable(1), new Text(input));
		mrDriver.withOutput(NullWritable.get(), new DoubleWritable(0.1692450000000001));
		
		mrDriver.runTest();
	}
}
