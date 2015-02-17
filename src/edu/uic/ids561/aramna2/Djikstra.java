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
		
		// Before even running the MapReduce job, we are calculating the Graph Statistics (How many nodes & edges, and the adjacency list for each node)
		
		try {
			new GraphStatistics().generateGraphStats();
		} catch (Exception e1) {
			System.out.println("Could not calculate graph stats");
			System.exit(1);
		}
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.Djikstra.class);

		int iterationCount = 0;
		long terminationValue = 1;
		
		while(terminationValue != 0){
		
			// Input format
			conf.setInputFormat(NodeInputFormat.class);
			
			// specify output types
			conf.setMapOutputKeyClass(LongWritable.class);
			conf.setMapOutputValueClass(Node.class);
	
			String input, output;
			
			if(0 == iterationCount){
				input = args[0];
			}
			else{
				input = args[1] + iterationCount;
			}
			
			output = args[1] + (iterationCount+1);
			
			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			// specify a mapper
			conf.setMapperClass(DjikstraMapper.class);
	
			// specify a reducer
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
