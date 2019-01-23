package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MaleEmploymentRateChangeMapper;
import com.revature.reduce.GlobalFemaleGraduationRateReducer;

public class MaleEmploymentChangeTest {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private String input;
	
	@Before
	public void setUp(){
		MaleEmploymentRateChangeMapper mecMapper = new MaleEmploymentRateChangeMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mecMapper);
	}
	
//	@Test
//	public void testHeader(){
//		String headerValue = GlobalFemaleGraduationRateTest.simulateInput("Country", "CountryCode", "Indicator Name", "Inidcator Code", "2000-01 *  2001-02 *  2002-03 *  2003-04 *  2004-05 *  2005-06 *  2006-07 *  2007-08 *  2008-09 *  2009-10 *  2010-11 *  2011-12 *  2012-13 *  2013-14 *  2014-15 *  2015-16 *  ");
//		mapDriver.withInput(new LongWritable(0), new Text(headerValue));
//		
//		String formattedHeaderKey = String.format("%-" + GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Country");
//		mapDriver.withOutput(new Text(formattedHeaderKey), new Text("2000-01 *  2001-02 *  2002-03 *  2003-04 *  2004-05 *  2005-06 *  2006-07 *  2007-08 *  2008-09 *  2009-10 *  2010-11 *  2011-12 *  2012-13 *  2013-14 *  2014-15 *  2015-16 *  "));
//		
//		mapDriver.runTest();
//	}
	
	@Test
	public void testMaleEmploymentChangeMapper() {
//		testHeader();
		
		input = GlobalFemaleGraduationRateTest.simulateInput("Australia","AUS","Employment to population ratio, 15+, male (%) (modeled ILO estimate)","SL.EMP.TOTL.SP.MA.ZS","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","67.4100036621094","65.9000015258789","65.3119964599609","66.4130020141602","67.5849990844727","67.3649978637695","66.9629974365234","67.177001953125","67.4700012207031","67.8079986572266","67.1060028076172","67.2880020141602","67.4580001831055","67.8529968261719","68.6490020751953","68.9260025024414","69.6640014648438","69.7919998168945","68.1989974975586","68.7870025634766","68.7320022583008","68.0989990234375","67.3509979248047","66.7649993896484","66.629997253418","66.7689971923828","");
		
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		String formattedKey = String.format("%-" + GlobalFemaleGraduationRateReducer.NUM_CHARACTERS_UNTIL_FIRST_VALUE + "s", "Australia");
		mapDriver.withOutput(new Text(formattedKey), new Text("-0.702%  *  +0.182%  *  +0.170%  *  +0.395%  *  +0.796%  *  +0.277%  *  +0.738%  *  +0.128%  *  -1.593%  *  +0.588%  *  -0.055%  *  -0.633%  *  -0.748%  *  -0.586%  *  -0.135%  *  "));
		
		mapDriver.runTest();
	}
}
