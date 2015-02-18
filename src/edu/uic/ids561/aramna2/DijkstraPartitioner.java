package edu.uic.ids561.aramna2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class DijkstraPartitioner implements Partitioner<LongWritable, Node> {

	@Override
	public void configure(JobConf arg0) {
		
	}

	@Override
	public int getPartition(LongWritable key, Node value, int numOfReduceTasks) {

		if(numOfReduceTasks == 0){
			return 0;
		}
		else{
			// this will map the intermediate key-value pairs among 3 reducers
			if( key.get() % 3 == 0 ){
				return 0;
			}
			else if( key.get() % 3 == 1 ){
				return 1 % numOfReduceTasks;
			}
			else{
				return 2 % numOfReduceTasks;
			}
		}
	}
}
