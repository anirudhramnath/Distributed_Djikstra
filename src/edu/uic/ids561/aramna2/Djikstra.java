package edu.uic.ids561.aramna2;

import java.util.Dictionary;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Djikstra {

	public static enum GrayCounter { NUMBER_OF_GRAY_NODES }
	
	public static void main(String[] args) throws InterruptedException {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.Djikstra.class);

		int iterationCount = 0;
		long terminationValue = 1;
		
		while(terminationValue != 0){
		
			// Input format
			conf.setInputFormat(NodeInputFormat.class);
			
			// TODO: specify output types
			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Node.class);
	
			String input, output;
			
			if(0==iterationCount){
				input = args[0];
			}
			else{
				input = args[1] + iterationCount;
			}
			
			output = args[1] + (iterationCount+1);
			
			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.addInputPath(conf, new Path(input));
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

		}
	}

}
