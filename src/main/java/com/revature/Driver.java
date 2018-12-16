package com.revature;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Driver {

	private static final Logger LOGGER = Logger.getLogger(Driver.class);
	private static final int NUMBER_OF_ARGUMENTS = 3;

	public static void main (String[] args) throws Exception {
		int exitCode = -1;
		
		if(args.length != NUMBER_OF_ARGUMENTS) {
			LOGGER.info("Usage: Driver <input_dir> <output_dir> <number> \n"
					+ "1: Countries where the number of female graduates is less than 30% \n"
					+ "2: Average increase of education attainmenet rate by females in the US \n"
					+ "3: \n"
					+ "4: \n"
					+ "5: \n");

			System.exit(exitCode);
		}

		else {
			String requirement = args[2];

			switch(requirement) {

			case "1":
				exitCode = ToolRunner.run(new GlobalFemaleGraduationRateJob(), args);
				break;

			case "2":
				exitCode = ToolRunner.run(new USAverageIncreaseInFemaleEducationAttainmentJob(), args);
				break;

			default: 
				LOGGER.info("Options are limited to 1 ~ 5");
				break;
			}

			System.exit(exitCode);
		}
	}
}