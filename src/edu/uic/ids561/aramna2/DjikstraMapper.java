package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DjikstraMapper extends MapReduceBase implements Mapper<LongWritable, Node, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Node value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

			System.out.println("success !!!");
			System.out.println("key:"+key.toString());
			System.out.println("list:"+value.adjacencyList.toString());
			System.out.println("distance:"+value.distanceFromSource);
			System.out.println("color:"+value.color.toString());
			System.out.println("parent:"+value.parent.toString());
			System.exit(1);
	}
}
