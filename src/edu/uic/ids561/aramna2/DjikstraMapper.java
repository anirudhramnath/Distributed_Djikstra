package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DjikstraMapper extends MapReduceBase implements Mapper<LongWritable, Node, LongWritable, Node> {

	@Override
	public void map(LongWritable key, Node value,
			OutputCollector<LongWritable, Node> output, Reporter reporter)
			throws IOException {
			
			// explode the adjacency list if GRAY node is found
			if( value.color.equalsIgnoreCase("GRAY") ){
				System.out.println("GRAY NODE:"+""+value.toString());
				String exploded[] = value.adjacencyList.trim().split(",");
				
				// emit a new GRAY node for each exploded value
				for(int i=0 ; i<exploded.length ; i++){
					
					// creating new GRAY node
					Node node = new Node("null", String.valueOf(Long.parseLong(value.distanceFromSource)+1), "GRAY", key.toString());
					
					output.collect(new LongWritable(Long.parseLong(exploded[i])), node);
				}
				
				// also emit the processed GRAY node as BLACK
				Djikstra.GrayCounter.counter --; // Decrease the counter value by 1
				value.color = "BLACK";
				output.collect(key, value);
			}
			
			// emit white nodes as is
			else if( value.color.equalsIgnoreCase("WHITE") ){
				output.collect(key, value);
			}
			
			// emit black nodes as is
			else if( value.color.equalsIgnoreCase("BLACK") ){
				output.collect(key, value);
			}

	}
}
