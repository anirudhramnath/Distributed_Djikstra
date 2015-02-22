package edu.uic.ids561.aramna2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Dijkstra {

	private static final String OUTPUT_FOLDER_NAME = "output";
	
	static class GrayCounter {
		public static int COUNTER = 1;
		public static int SOURCE_NODE;
		public static boolean SOURCE_NODE_FOUND = false;
		public static int TOTAL_NUM_NODES;
	}
	
	public static void main(String[] args) {
		
		// check if two command line arguments have been received
		if(args.length != 2){
			System.out.println("Error: Program expecting two command line arguments");
			System.exit(1);
		}
		else{
			// set `args[1]` to `SOURCE_NODE` variable
			try{
				GrayCounter.SOURCE_NODE = Integer.parseInt(args[1]);
			}
			catch(Exception ex){
				System.out.println("args[1] should be an number");
				System.exit(1);
			}
		}
		
		// Before running the MapReduce job, we are calculating the Graph Statistics (How many nodes & edges, and the adjacency list for each node)
		try {
			new GraphStatistics().generateGraphStats();
		} catch (Exception e1) {
			System.out.println("Could not calculate graph stats. Please check if you have created a folder named `stats`");
			System.exit(1);
		}
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.Dijkstra.class);

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
				input = OUTPUT_FOLDER_NAME + iterationCount;
			}
			
			output = OUTPUT_FOLDER_NAME + (iterationCount+1);
			
			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			// specify a mapper
			conf.setMapperClass(DijkstraMapper.class);
			conf.setNumMapTasks(3);
	
			// specify a reducer
			conf.setReducerClass(DijkstraReducer.class);
			conf.setNumReduceTasks(3);
			
			// specify partitioner
			conf.setPartitionerClass(DijkstraPartitioner.class);
			
			client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			iterationCount ++;
			terminationValue = GrayCounter.COUNTER;
			
			// if the source node specified in the command line argument does not exist in the graph, then throw error and terminate the program
			if( !GrayCounter.SOURCE_NODE_FOUND ){
				System.out.println("Error: source node not found in graph");
				System.exit(1);
			}
		}
	}

}
