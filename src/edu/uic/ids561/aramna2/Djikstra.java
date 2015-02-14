package edu.uic.ids561.aramna2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Djikstra {

	public static enum GrayCounter { NUMBER_OF_GRAY_NODES }
	
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.Djikstra.class);

		// Input format
		conf.setInputFormat(NodeInputFormat.class);
		
		// TODO: specify output types
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Node.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

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
