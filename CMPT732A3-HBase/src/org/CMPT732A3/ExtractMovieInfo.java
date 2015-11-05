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
		String movieTable = args[0];// movieTable = "weipuz_movieInfo" or
		// "weipuz_movieInfo_1M";
		String input = args[1]; // String filename = "movies.dat"; or
		// "1M/movies.dat";
		Configuration config = HBaseConfiguration.create();
		// Create table
		HBaseAdmin admin = new HBaseAdmin(config);

		HTableDescriptor htd = new HTableDescriptor(
				TableName.valueOf(movieTable));
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}

		// Run some operations -- a put, and a get
		HTable table = new HTable(config, tablename);
		if (table.isAutoFlush()) {
			table.setAutoFlushTo(false);
			System.out.println("Enable Client side flush;");
		}
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line;
		int count = 0;
		while ((line = br.readLine()) != null) {
			// process the line.
			String aline[] = line.split("::");
			// continue if line incomplete or reach the last empty line;
			if (aline.length < 2) {
				System.out.println(line);
				continue;
			}

			// put the first two element into table, rowkey is movieID, first
			// element is Title;
			byte[] row1 = Bytes.toBytes(aline[0]);
			Put p1 = new Put(row1);
			byte[] databytes = Bytes.toBytes("data");
			p1.add(databytes, Bytes.toBytes("Title"), Bytes.toBytes(aline[1]));

			// put the third element into table;
			if (aline.length == 3) {
				p1.add(databytes, Bytes.toBytes("Genres"),
						Bytes.toBytes(aline[2]));
			}
			table.put(p1);
			if (count == 100) {
				count = 0;
				table.flushCommits();
			}

			count++;
		}
		table.flushCommits();
		br.close();

		// Drop the table
		// admin.disableTable(tablename);
		// admin.deleteTable(tablename);

	}

}
