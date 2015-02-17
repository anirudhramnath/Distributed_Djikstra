package edu.uic.ids561.aramna2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is the data structure used to represent a `Node`.
 * It will used in the custom InputFormat - `NodeInputFormat` which will pass an object of this class as value to the `map` function.
 * @author aramna2
 *
 */
public class Node implements Writable{
	
	String adjacencyList;
	String distanceFromSource;
	String color;
	String parent;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		adjacencyList = in.readUTF();
		distanceFromSource = in.readUTF();
		color = in.readUTF();
		parent = in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(adjacencyList);
		out.writeUTF(distanceFromSource);
		out.writeUTF(color);
		out.writeUTF(parent);
	}
	
	public Node(String adjacencyList, String distanceFromSource, String color,
			String parent) {
		super();
		this.adjacencyList = adjacencyList;
		this.distanceFromSource = distanceFromSource;
		this.color = color;
		this.parent = parent;
	}
	
	public Node(){
		this("", "Integer.MAX_VALUE", "WHITE", "");
	}
	
	public String toString(){
		return adjacencyList+"|"+distanceFromSource+"|"+color+"|"+parent;
	}
}
