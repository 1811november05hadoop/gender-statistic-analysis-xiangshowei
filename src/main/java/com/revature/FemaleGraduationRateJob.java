package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.revature.map.FemaleGraduationRateMapper;

public class FemaleGraduationRateJob extends Configured implements Tool {
	
	private static final Logger LOGGER = Logger.getLogger(FemaleGraduationRateJob.class);
	private static final int NUMBER_OF_ARGUMENTS = 2;

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != NUMBER_OF_ARGUMENTS) {
			LOGGER.info("Usage: FemaleGraduationRateJob <input_dir> <output_dir>");
			
			return -1;
		}
		
		else {
			Job job = new Job();
			
			job.setJobName("Female Graduation Rate");
			job.setJarByClass(FemaleGraduationRateJob.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			/* output path is set to a sub-directory in target folder for convenience
			 * as the target folder gets deleted after each clean package goal
			 */
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass(FemaleGraduationRateMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			boolean jobComplete = job.waitForCompletion(true);
			
			return jobComplete ? 0 : 1;
		}
	}

}
