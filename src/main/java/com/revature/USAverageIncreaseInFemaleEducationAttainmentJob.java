package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.USAverageIncreaseInFemaleEducationAttainmentMapper;
import com.revature.reduce.USAverageIncreaseInFemaleEducationAttainmentReducer;

public class USAverageIncreaseInFemaleEducationAttainmentJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
			Job job = new Job();
			
			job.setJobName("Female Graduation Rate in the US from the year 2000");
			job.setJarByClass(USAverageIncreaseInFemaleEducationAttainmentJob.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(USAverageIncreaseInFemaleEducationAttainmentMapper.class);
			job.setReducerClass(USAverageIncreaseInFemaleEducationAttainmentReducer.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			boolean jobComplete = job.waitForCompletion(true);

			return jobComplete ? 0 : 1;
	}
}
