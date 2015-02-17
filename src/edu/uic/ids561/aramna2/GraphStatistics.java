package edu.uic.ids561.aramna2;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class GraphStatistics {

	public void generateGraphStats() throws Exception{
		FileInputStream fis = null;
        BufferedReader reader = null;
        PrintWriter writer = null;
        
        int nodeCount = 0;
        int edgeCount = 0;
        
        try {
        	
        	// read the file
            fis = new FileInputStream("input/input.txt");
            reader = new BufferedReader(new InputStreamReader(fis));
          
            // create PrintWriter object to write to a new file called `GraphStats.txt`
            writer = new PrintWriter("stats/GraphStats.txt", "UTF-8");
            
            String line;
            
            while(true){
            	
                line = reader.readLine();
                
                // read till end of file
                if(line == null){
                	break;
                }
                
                // Increment nodeCount by 1 since each line is a new node
                nodeCount += 1;
                
                String[] split = line.split("\t");
                String[] splitAdjList = split[1].split("\\|");
                
                // write adjacency list for each node to the `GraphStats` file
                writer.println(split[0]+"\t"+splitAdjList[0]);
                
                // calculate number of edges for each node and keep adding them in the `edgeCount` variable
                edgeCount += splitAdjList[0].split(",").length;
            }
            
            // write number of nodes and edges to the file
            // we are doing `edgeCount / 2` since the graph is undirected
            writer.println("\n\n\nGraph Stats:\nNumber of nodes in file: "+nodeCount+"\nNumber of edges in file: "+edgeCount/2);
          
        } catch (Exception ex) {
        	throw ex;
          
        } finally {
                reader.close();
                fis.close();
                writer.close();
            }

	}
}
