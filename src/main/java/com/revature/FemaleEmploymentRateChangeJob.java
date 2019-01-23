package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.FemaleEmploymentRateChangeMapper;

public class FemaleEmploymentRateChangeJob extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();

		job.setJobName("Change in employment rates for females since 2000");
		job.setJarByClass(FemaleEmploymentRateChangeJob.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleEmploymentRateChangeMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean jobComplete = job.waitForCompletion(true);

		return jobComplete ? 0 : 1;
	}
}
