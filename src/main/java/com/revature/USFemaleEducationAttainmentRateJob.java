package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.revature.map.USFemaleEducationAttainmentRateMapper;
import com.revature.reduce.USFemaleEducationAttainmentRateReducer;

public class USFemaleEducationAttainmentRateJob extends Configured implements Tool {
	
	private static final Logger LOGGER = Logger.getLogger(USFemaleEducationAttainmentRateJob.class);
	private static final int NUMBER_OF_ARGUMENTS = 2;

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != NUMBER_OF_ARGUMENTS) {
			LOGGER.info("Usage: USFemaleEducationAttainmentJob <input_dir> <output_dir>");

			return -1;
		}
		
		else {
			Job job = new Job();
			
			job.setJobName("Female Graduation Rate in the US from the year 2000");
			job.setJarByClass(USFemaleEducationAttainmentRateJob.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(USFemaleEducationAttainmentRateMapper.class);
			job.setReducerClass(USFemaleEducationAttainmentRateReducer.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			boolean jobComplete = job.waitForCompletion(true);

			return jobComplete ? 0 : 1;
		}
	}
}
