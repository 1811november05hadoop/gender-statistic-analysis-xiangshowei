package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.USFemaleDelayedSchoolEnrollmentRateMapper;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateCombiner;
import com.revature.reduce.USFemaleDelayedSchoolEnrollmentRateReducer;

public class USFemaleDelayedSchoolEnrollmentRateJob extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
			Job job = new Job();
			
			job.setJobName("Female School Enrollment Rate in the US since 2010");
			job.setJarByClass(USFemaleDelayedSchoolEnrollmentRateJob.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(USFemaleDelayedSchoolEnrollmentRateMapper.class);
			job.setCombinerClass(USFemaleDelayedSchoolEnrollmentRateCombiner.class);
			job.setReducerClass(USFemaleDelayedSchoolEnrollmentRateReducer.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			boolean jobComplete = job.waitForCompletion(true);

			return jobComplete ? 0 : 1;
	}
}
