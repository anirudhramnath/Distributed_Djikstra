package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class NodeRecordReader implements RecordReader<LongWritable, Node>{

	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	public NodeRecordReader(JobConf job, FileSplit split) throws IOException {
		
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}
	
	@Override
	public boolean next(LongWritable key, Node value) throws IOException {
		
		// Get the next line
		if (!lineReader.next(lineKey, lineValue)) { 
			return false;
		}
		
		// parse the lineValue which is in the format: source<tab>adjacency list | distance from source | color | parent node
		String [] pieces = lineValue.toString().split("	");
		
		if (pieces.length != 2) {
			throw new IOException("Invalid record received");
		}
		
		String[] valueSplit = pieces[1].split("\\|");
		
		if(valueSplit.length != 4){
			throw new IOException("Invalid record received");
		}

		String adjacencyList = null;
		String distanceFromSource = null;
		String color = null;
		String parent = null;
		
		long inputKey = 0;

		try{
			// parsing the values from the input file
			inputKey = Long.parseLong(pieces[0].trim());
			adjacencyList = valueSplit[0].trim();
			distanceFromSource = valueSplit[1].trim();
			color = valueSplit[2].trim();
			parent = valueSplit[3].trim();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		// overwrite the output values and set appropriate value from the input file
		key.set(inputKey);
		value.adjacencyList = adjacencyList;
		value.distanceFromSource = distanceFromSource;
		value.color = color;
		value.parent = parent;
		
		return true;
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Node createValue() {
		return new Node();
	}

	@Override
	public long getPos() throws IOException {
		return lineReader.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}

}
