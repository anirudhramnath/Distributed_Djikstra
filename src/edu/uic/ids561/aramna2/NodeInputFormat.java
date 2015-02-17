package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class NodeInputFormat extends FileInputFormat<LongWritable, Node>{

	@Override
	public RecordReader<LongWritable, Node> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {

		return new NodeRecordReader(arg1, (FileSplit) arg0);
	}

	public RecordReader<LongWritable, Node> createRecordReader( InputSplit input, JobConf job,
			Reporter reporter) throws IOException { 
			reporter.setStatus(input.toString());
			return new NodeRecordReader(job, (FileSplit) input); 
	}
		
}
