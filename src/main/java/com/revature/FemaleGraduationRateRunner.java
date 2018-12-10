package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class FemaleGraduationRateRunner {

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new Configuration(), new FemaleGraduationRateJob(), args);
		System.exit(exitCode);
	}
}
