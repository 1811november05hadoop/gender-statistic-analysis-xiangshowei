package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class USAverageIncreaseInFemaleEducationAttainmentReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable>{
	
	public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double sum = 0;
		int numDataPoints = 0;
		
		for (DoubleWritable value : values) {
			if(value.get() > 0) {
				sum+= value.get();
				numDataPoints++;
			}
		}
		
		context.write(key, new DoubleWritable(sum / numDataPoints));
	}

}