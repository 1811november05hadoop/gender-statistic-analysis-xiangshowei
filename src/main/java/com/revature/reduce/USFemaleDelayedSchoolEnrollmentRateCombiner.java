package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USFemaleDelayedSchoolEnrollmentRateCombiner extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder outputValue = new StringBuilder();

		for(Text value : values) {
			String enrollmentRate = value.toString();
			if(!enrollmentRate.isEmpty()) {
				if(enrollmentRate.charAt(0) == 'g') {
					outputValue.append(enrollmentRate.replaceFirst("g", "") + ":");
				}

				if (enrollmentRate.charAt(0) == 'n') {
					outputValue.append(enrollmentRate.replaceFirst("n", ""));
				}
			}
		}

		context.write(key, new Text(outputValue.toString()));
	}
}
