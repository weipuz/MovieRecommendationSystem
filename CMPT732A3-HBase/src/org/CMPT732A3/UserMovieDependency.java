package org.CMPT732A3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;

public class UserMovieDependency {
	private final static IntWritable one = new IntWritable(1);
	
	/*-------------------------Mapper 1------------------------------------*/
	static class UserMapper_1 extends TableMapper<Text, Text>{
		private byte[] columnFamily;
		private byte[] qualifier;
		
		@Override
		protected void setup(Context context){
			// get column family and qualifer from context.getConfiguration()
			Configuration conf = context.getConfiguration();
			columnFamily = Bytes.toBytes(conf.get("conf.columnFamily",null));
			qualifier = Bytes.toBytes(conf.get("conf.qualifier",null));
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			byte [] b = row.get();
			String s = new String(b, "UTF-8");
			String[] row_array = s.split(":");
			String movierowkey =  row_array[0];
			if(row_array.length == 1){
				byte[] gernes = value.getValue(columnFamily,qualifier);
				if(gernes!=null){
					String gernes_string = new String(gernes, "UTF-8");
					context.write(new Text(movierowkey), new Text("gernes:"+ gernes_string));
				}
			}
			else{
				byte [] userID =  Bytes.toBytes(row_array[1]);
				String userID_string = new String(userID, "UTF-8");
				context.write(new Text(movierowkey), new Text("user:" + userID_string));
			}
		}
	}
	
	/*-------------------------Reduce 1------------------------------------*/
	// reducer1 will take movieID as key and a pair of info as values, will generate (movieID:userID, Genres) to a TMP Hbase
	static class UserReducer_1 extends TableReducer<Text, Text, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String gernes = null;
			List<String> users = new ArrayList<String>();
			for(Text value : values){
				String[] data = value.toString().split(":");
				if(data.length != 2){
					continue;
				}
				String type = data[0];
				if(type.equals("gernes")){
					gernes = data[1];
				}
				else if (type.equals("user")){
					users.add(data[1]);
				}	
			}
			
			if(gernes == null || users.isEmpty()){
				return;
			}
			
			for(String user : users){
				byte [] rowkey = Bytes.toBytes( key.toString() + ":" + user);  //movieID + : + userID as row key;
				Put put = new Put(rowkey);
				put.add(Bytes.toBytes("data"), Bytes.toBytes("Gernes"), Bytes.toBytes(gernes));
				context.write(new ImmutableBytesWritable(rowkey), put);
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String movieTable = "weipuz_movieInfo";
		String ratingTable = "weipuz_ratingInfo";
		String tempTable = "weipuz_temp";
		String columnFamily = "data";
		String qualifier = "Genres";
		
		System.out.println(movieTable + " " + ratingTable + " " + tempTable + " " +  qualifier);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		
		// we can pass column family and qualifier in conf to reducers 
		conf.set("conf.columnFamily", columnFamily);
		conf.set("conf.qualifier", qualifier);
		
		List<Scan> scans = new ArrayList<Scan>();
		 
		Scan scan1 = new Scan();
		scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(movieTable));
		System.out.println(scan1.getAttribute("scan.attributes.table.name"));
		scans.add(scan1);
 
		Scan scan2 = new Scan();
		scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(ratingTable));
		System.out.println(scan2.getAttribute("scan.attributes.table.name"));
		scans.add(scan2);
		
		Job job = Job.getInstance(conf);
	    TableMapReduceUtil.initTableMapperJob(scans, UserMapper_1.class,
	    		Text.class, Text.class, job);
	    TableMapReduceUtil.initTableReducerJob(tempTable, 
	    		UserReducer_1.class, job);
	    
	  //  job.setReducerClass(RatingReduce.class);
	  //  FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	   // job.setOutputKeyClass(IntWritable.class);
	   // job.setOutputValueClass(DoubleWritable.class);
	    
	    job.setJarByClass(ExtractRatingCount.class);
	    job.waitForCompletion(true);
		
	}
}
