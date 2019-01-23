package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USFemaleDelayedSchoolEnrollmentRateReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double enrollmentRateDelta = 0;

		for (Text value : values) {
			String[] input = value.toString().split(":");


			String grossEnrollmentRate = input[0];
			String netEnrollmentRate = input[1];

			if(!grossEnrollmentRate.isEmpty() && !netEnrollmentRate.isEmpty()) { 
				enrollmentRateDelta = Double.parseDouble(input[0]) - 
						Double.parseDouble(input[1]);
			}

			context.write(key, new DoubleWritable(enrollmentRateDelta));
		}
	}
}
