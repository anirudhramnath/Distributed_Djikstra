package edu.uic.ids561.aramna2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

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
