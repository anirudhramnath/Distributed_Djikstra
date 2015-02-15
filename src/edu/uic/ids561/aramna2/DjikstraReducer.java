package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uic.ids561.aramna2.Djikstra.GrayCounter;

public class DjikstraReducer extends MapReduceBase implements Reducer<LongWritable, Iterator<Node>, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterator<Iterator<Node>> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		
		String darkestColor="WHITE";
		int minDistance = Integer.MAX_VALUE;
		String parent="null";
		String list="";
		
		while(values.hasNext()){
			
			Node node = (Node) values.next();
			
			String distanceInNode = node.distanceFromSource;
			
			int distanceInteger = -1;
			
			if( ! distanceInNode.equalsIgnoreCase("Integer.MAX_VALUE") ){
				distanceInteger = Integer.parseInt(distanceInNode);
			}
			
			if(node.adjacencyList != null && !node.adjacencyList.equalsIgnoreCase("")){
				list = node.adjacencyList;
			}
			
			if( distanceInteger != -1 && distanceInteger < minDistance){
				minDistance = distanceInteger;
				parent = node.parent;
			}
			
			if(darkestColor.equalsIgnoreCase("WHITE") && (node.color.equalsIgnoreCase("GRAY") || node.color.equalsIgnoreCase("BLACK"))){
				darkestColor = node.color;
			}
			else if(darkestColor.equalsIgnoreCase("GRAY") && (node.color.equalsIgnoreCase("BLACK"))){
				darkestColor = node.color;
			}
			else if(node.color.equalsIgnoreCase("BLACK")){
				darkestColor = "BLACK";
			}
		}
		
		String minDistanceString = "Integer.MAX_VALUE";
		
		if(minDistance != Integer.MAX_VALUE){
			minDistanceString = String.valueOf(minDistance);
		}
		
		if(darkestColor.equalsIgnoreCase("GRAY")){
			Djikstra.GrayCounter.counter ++;
			System.out.println("Incremented:"+Djikstra.GrayCounter.counter);
		}
		
		Node outputNode = new Node(list, minDistanceString, darkestColor, parent);
		
		output.collect( key, new Text(outputNode.toString()) );
	}

}
