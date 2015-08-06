package com.analyzers.sampler;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class toolrunner extends Configured implements Tool {
	//public String str;
	public static void main(String[] args) throws Exception {
		//System.out
			//	.println("************************** In main method ****************");

		Configuration configuration = new Configuration();
	 toolrunner dateTR = new toolrunner();

		int exitCode = ToolRunner.run(configuration, dateTR, args);
		System.exit(exitCode);
	}
	public int run(String[] arg0) throws Exception {
		//str=arg0[2];

		Configuration conf = getConf();
		//int a= Integer.parseInt(arg0[2]);
		conf.set("yourcustom.property", arg0[2]);
		/*if (arg0.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}*/
		Job job = new Job(conf,
				"Sampler z "
						+ Calendar.getInstance().getTime());
		//int numberOfReduceTasks = 1;
		//int linesPerMapper = 1;
	/*	for (Entry<String, String> entry : conf) {
			
//			if(entry.getKey().contains("mapreduce.")) {
//				System.out.println("Key is "+entry.getKey()
//						+" and value is "+entry.getValue());
//			}
			

			if (entry.getKey().equalsIgnoreCase("reducerTaskHandler")) {
				System.out.println("Value for reducer parameter is "
						+ Integer.valueOf(entry.getValue()));
				numberOfReduceTasks = Integer.valueOf(entry.getValue());
			}
			String currentKeyUnderIteration = entry.getKey();
			if (currentKeyUnderIteration.equalsIgnoreCase("mapperTaskHandler")) {
				System.out.println("Value for passed parameter is "
						+ Integer.valueOf(entry.getValue()));
				linesPerMapper = Integer.valueOf(entry.getValue());
			}
			// job.getConfiguration().setInt(entry.getKey(),
			// Integer.valueOf(entry.getValue()));
		}*/
		System.out
				.println("************************** After property populator ****************");

		// Way of setting the inbuilt properties e.g. Number of reducer task
		// property i.e mapred.reduce.tasks as 2.

		// job.getConfiguration().setInt("mapred.reduce.tasks", 2);

		// To set the number of splitters by setting the number of lines per
		// map.

	/*	job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);*/
		//job.getConfiguration().setInt(
			//	"mapreduce.input.lineinputformat.linespermap",linesPerMapper);

		job.setJarByClass(toolrunner.class);
		job.setMapperClass(maptask.class);
		//job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(reducetask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Can be set by method also within class and passing the value as
		// parameter.
		//job.setNumReduceTasks(numberOfReduceTasks);

		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//System.out.println(recudertask.q);
		return 0;
	}
	

}

