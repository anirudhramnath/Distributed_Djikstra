package edu.uic.ids561.aramna2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Djikstra {

	static class GrayCounter {
		public static int counter = 1;
	}
	
	public static void main(String[] args) {
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.Djikstra.class);

		int iterationCount = 0;
		long terminationValue = 1;
		
		while(terminationValue != 0){
		
			// Input format
			conf.setInputFormat(NodeInputFormat.class);
			
			// TODO: specify output types
			conf.setMapOutputKeyClass(LongWritable.class);
			conf.setMapOutputValueClass(Node.class);
	
			String input, output;
			
			if(0 == iterationCount){
				input = args[0];
			}
			else{
				input = args[1] + iterationCount;
				System.out.println("**Input file**"+input);
			}
			
			output = args[1] + (iterationCount+1);
			
			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			// TODO: specify a mapper
			conf.setMapperClass(DjikstraMapper.class);
	
			// TODO: specify a reducer
			conf.setReducerClass(DjikstraReducer.class);
	
			client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			iterationCount ++;
			terminationValue = GrayCounter.counter;
		}
	}

}
