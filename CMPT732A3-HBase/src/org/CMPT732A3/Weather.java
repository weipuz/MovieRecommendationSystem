package org.CMPT732A3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.*;

public class Weather {
	public static void main(String[] args) throws IOException {
		long starttime = System.currentTimeMillis();
		Configuration config = HBaseConfiguration.create();
		// Create table
		//HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("avahdat_weather"));
		byte [] tablename = htd.getName();
		HTable table = new HTable(config, tablename);
		/*
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("weipuz_test1"));
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
		byte [] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		byte [] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value1"));
		p1.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value2"));
		table.put(p1);
		*/
		
		List<Filter> filters = new ArrayList<Filter>(2);
		byte[] colfam = Bytes.toBytes("data");
		byte[] fakeValue = Bytes.toBytes("0");
		byte[] colA = Bytes.toBytes("SNOW");
		//byte[] colB = Bytes.toBytes("col_B");

		SingleColumnValueFilter filter1 = 
		    new SingleColumnValueFilter(colfam, colA , CompareOp.NOT_EQUAL, fakeValue);  
		filter1.setFilterIfMissing(true);
		filters.add(filter1);
		
		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
			      new RegexStringComparator("USC."));
		filters.add(filter2);
		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		//scan.setCaching(20);
		ResultScanner sc = table.getScanner(scan);
		// ??
		// fix the output format to the following format
		// rowKey,  columnFamily:qualifier, value
		int max =0; 
		int count =0;
		
			for (Result result = sc.next(); (result != null) ; result = sc.next()) {
				
				//Get g = new Get(result.getRow());
				//Result Entireresult = table.get(g);
				List<Cell> cellsc = result.listCells();
				//System.out.println(Entireresult);
				
				for (Cell cell : cellsc){
					
					//String row = new String(CellUtil.cloneRow(cell));
					//String fam = new String(CellUtil.cloneFamily(cell));
					//String qualifier = new String(CellUtil.cloneQualifier(cell));
					//System.out.println(row + "  " + qualifier);
					int value = Integer.parseInt(new String(CellUtil.cloneValue(cell)));
					if(value >max){
						max = value;
					}
					/*
					if(qualifier.equals("SNOW")){
						count++;
						//System.out.println(qualifier);
						//System.out.println(count);
						int value = Integer.parseInt(new String(CellUtil.cloneValue(cell)));;
						if(value >max){
							max = value;
						}
					}
					*/
					
				}
				
				
				/*result.advance();
				
				
			*/
			}
			System.out.println(count);
			System.out.println("Get: " + Integer.toString(max));
		
		long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println("time used: "+ time/1000 + " seconds");
		// Let's put more data the table
		/*
		byte [] row2 = Bytes.toBytes("row2");
		Put p2 = new Put(row2);
		p2.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value3"));
		table.put(p2);
		
		byte [] row3 = Bytes.toBytes("row3");
		Put p3 = new Put(row3);
		p3.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value4"));
		table.put(p3);
		
		// ??
		// Write a scan on whole table.
		// Put your code here
		// ...
		
		// Drop the table
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		*/
	}
}