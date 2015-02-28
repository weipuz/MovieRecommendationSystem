package org.CMPT732A3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class ExtractMovieInfo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration config = HBaseConfiguration.create();
		// Create table
		HBaseAdmin admin = new HBaseAdmin(config);
		
		
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("weipuz_movieInfo"));
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte [] tablename = htd.getName();
		HTableDescriptor [] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		// Run some operations -- a put, and a get
		HTable table = new HTable(config, tablename);
		String filename = "movies.dat";
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		int row_index = 0;
		while ((line = br.readLine()) != null) {
		   // process the line.
			String aline[] = line.split("::");
			String row = Integer.toString(row_index);
			row_index++;
			
			byte [] row1 = Bytes.toBytes(row);
			Put p1 = new Put(row1);
			byte [] databytes = Bytes.toBytes("data");
			
			p1.add(databytes, Bytes.toBytes("ID"), Bytes.toBytes(aline[0]));
			p1.add(databytes, Bytes.toBytes("Title"), Bytes.toBytes(aline[1]));
			p1.add(databytes, Bytes.toBytes("Genres"), Bytes.toBytes(aline[2]));
				
			table.put(p1);		
			
			
			
		}
		br.close();
		
		// Drop the table
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
			
		

	}

}
