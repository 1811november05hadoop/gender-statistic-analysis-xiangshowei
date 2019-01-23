package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleEmploymentRateChangeMapper;
import com.revature.reduce.GlobalFemaleGraduationRateReducer;

public class FemaleEmploymentChangeTest {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private String input;
	
	@Before
	public void setUp(){
		FemaleEmploymentRateChangeMapper fecMapper = new FemaleEmploymentRateChangeMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(fecMapper);
	}
	
	@Test
	public void testFemaleEmploymentChangeMapper() {
		input = GlobalFemaleGraduationRateTest.simulateInput("Australia","AUS","Employment to population ratio, 15+, female (%) (modeled ILO estimate)","SL.EMP.TOTL.SP.FE.ZS","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","47.2599983215332","46.7900009155273","46.6559982299805","47.7299995422363","49.4080009460449","49.3680000305176","49.3650016784668","49.8979988098145","50.3050003051758","51.4049987792969","51.6150016784668","51.9389991760254","52.6450004577637","52.6640014648438","54.0379981994629","54.7879981994629","55.4269981384277","55.9710006713867","55.6020011901855","55.5460014343262","55.8810005187988","55.7089996337891","55.4070014953613","55.0400009155273","55.0139999389648","55.0979995727539","");
		
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		String formattedKey = String.format("%-" + GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Australia");
		mapDriver.withOutput(new Text(formattedKey), new Text("+0.210%  *  +0.324%  *  +0.706%  *  +0.019%  *  +1.374%  *  +0.750%  *  +0.639%  *  +0.544%  *  -0.369%  *  -0.056%  *  +0.335%  *  -0.172%  *  -0.302%  *  -0.367%  *  -0.026%  *  "));
		
		mapDriver.runTest();
	}
}
