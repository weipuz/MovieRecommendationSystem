package org.CMPT732A3;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.mapreduce.*; 
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ExtractMovieSimilar {
	static private final Path TMP_DIR = new Path(ExtractMovieSimilar.class.getSimpleName() + "_TMP_");
	
	/*-------------------------Mapper 0------------------------------------*/
	static class Mapper_0 extends TableMapper<IntWritable, IntPair>{
		private byte[] columnFamily;
		private byte[] qualifier;
		//private byte[] qualifier1;
		
		@Override
		protected void setup(Context context){
			// get column family and qualifer from context.getConfiguration()
			Configuration conf = context.getConfiguration();
			columnFamily = Bytes.toBytes(conf.get("conf.columnFamily",null));
			qualifier = Bytes.toBytes(conf.get("conf.qualifier",null));

			//qualifier1 = Bytes.toBytes(conf.get("conf.qualifier","Count"));
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			byte [] b = row.get();
			String s = new String(b, "UTF-8");
			String [] s_list = s.split(":");
			
			//KeyValue [] keyvalue = value.;
			
			if(s_list.length==2){//rating hbase
				String movieID_string = s.split(":")[0];
				String userID_string = s.split(":")[1];
				int movieID = Integer.valueOf(movieID_string);
				int userID = Integer.valueOf(userID_string);
				byte[] rating_byte = value.getValue(columnFamily,qualifier);
				int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
				IntPair pair = new IntPair(userID,rating);
				context.write(new IntWritable(movieID), pair);
			}else{//movie hbase
				String movieID_string = s.split(":")[0];
				int movieID = Integer.valueOf(movieID_string);
				int userID =0;
				
				
				byte[] rating_byte = value.getValue(Bytes.toBytes("data"), Bytes.toBytes("Count")); ////problem here!!!!
				if(rating_byte!=null){
					
					int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
					System.out.println(rating);
					//System.out.println(rating_byte);
					System.out.println("...........");
					IntPair pair = new IntPair(userID,rating);
					context.write(new IntWritable(movieID), pair);
				}
				
			}
			//String movieID_string = s.split(":")[0];
			//String userID_string = s.split(":")[1];
			//int movieID = Integer.valueOf(movieID_string);
			//int userID = Integer.valueOf(userID_string);
			//byte[] rating_byte = value.getValue(columnFamily,qualifier);
			//String rating_string = new String(rating_byte, "UTF-8");
			
			
			//String movie_rating = movieID_string+ ","+rating_string;
			//int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
			//IntPair pair = new IntPair(movieID,rating);
			//context.write(new IntWritable(userID), pair);
			
		}
	}
	
	/*-------------------------Reduce 0------------------------------------*/
	// reducer will take movieID as key and ratings as values, will generate <movieID, averageRating>
	static class Reduce_0 extends Reducer<IntWritable, IntPair, IntWritable, IntTriple> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
			int n =0;
			ArrayList <Integer> userlist= new ArrayList<Integer>();
			ArrayList <Integer> ratinglist= new ArrayList<Integer>();
			for(IntPair value :values){
				
				if(value.getFirst().get()==0){
					n = value.getSecond().get();
				}else{
					userlist.add(value.getFirst().get());
					ratinglist.add(value.getSecond().get());	
				}
			}
			
			
			
			for(int i=0; i<userlist.size();i++){
				
					IntTriple triple = new IntTriple(key.get(), ratinglist.get(i), n);
					context.write(new IntWritable(userlist.get(i)), triple);
			}
			
		}
	}
	
	/*-------------------------Mapper 1------------------------------------*/
	static class Mapper_1 extends Mapper <LongWritable, Text, IntWritable, IntTriple>{
		
		
		/*
		@Override
		protected void setup(Context context){
			// get column family and qualifer from context.getConfiguration()
			Configuration conf = context.getConfiguration();
			columnFamily = Bytes.toBytes(conf.get("conf.columnFamily",null));
			qualifier = Bytes.toBytes(conf.get("conf.qualifier",null));
		}
		*/
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
			String[] in = value.toString().split("\t");
			//String userID = in[0];
			String[] movieInfo = in[1].split(":");
			//String movieID = movieInfo[0];
			//String rating = movieInfo[1];
			//String n_string = movieInfo[2];
			
			int userID = Integer.parseInt(in[0]);
			int movieID = Integer.parseInt(movieInfo[0]);
			int rating = Integer.parseInt(movieInfo[1]);
			int n = Integer.parseInt(movieInfo[2]);
			
			context.write(new IntWritable(userID), new IntTriple(movieID, rating,n));
			
		}
	}
	
	
	/*-------------------------Reduce 1------------------------------------*/
	// reducer will take movieID as key and ratings as values, will generate <movieID, averageRating>
	static class Reduce_1 extends Reducer<IntWritable, IntTriple, IntPair, IntForthPair> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntTriple> values, Context context) throws IOException, InterruptedException {
			//double sum = 0;
			//Iterator<IntPair> list =  values.iterator();
			int count=0;
			int count2=0;
			ArrayList <Integer> movielist= new ArrayList<Integer>();
			ArrayList <Integer> ratinglist= new ArrayList<Integer>();
			ArrayList <Integer> n_list= new ArrayList<Integer>();

			for(IntTriple value :values){
				movielist.add(value.getFirst().get());
				ratinglist.add(value.getSecond().get());
				n_list.add(value.getThird().get());
			}
			
			for(int i=0; i<movielist.size();i++){
				int movie_1 = movielist.get(i);
				int rating_1 = ratinglist.get(i);
				int n_1 = n_list.get(i);
				//System.out.println("movie_1: "+ Integer.toString(movie_1) +"  rating_1: "+ Integer.toString(rating_1));
				count++;
				for(int j=i+1; j<movielist.size();j++){
					
					count2++;
					int movie_2 = movielist.get(j);
					int rating_2 = ratinglist.get(j);
					int n_2 = n_list.get(j);
					//System.out.println("movie_2: "+ Integer.toString(movie_2) +"  rating_2: "+ Integer.toString(rating_2));
					if(movie_1<movie_2){
						context.write(new IntPair(movie_1, movie_2), new IntForthPair(rating_1, n_1,rating_2,n_2));
					}else{
						context.write(new IntPair(movie_2, movie_1), new IntForthPair(rating_2, n_2, rating_1,n_1));
					}
					//context.write(new IntPair(movie_1, movie_2), new IntPair(rating_1, rating_2));	
					//System
				}
			}
		}
	}
	
	
	/*-------------------------Mapper 2------------------------------------*/
	static class Mapper_2 extends Mapper <LongWritable, Text,IntPair,IntForthPair>{
		
		
		
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
			String[] in = value.toString().split("\t");
			String[] movieID = in[0].split(":");
			String[] rating = in[1].split(":");
			int movie_1 = Integer.parseInt(movieID[0]);
			int movie_2 = Integer.parseInt(movieID[1]);
			int rating_1 = Integer.parseInt(rating[0]);
			int n_1 = Integer.parseInt(rating[1]);
			int rating_2 = Integer.parseInt(rating[2]);
			int n_2 = Integer.parseInt(rating[3]);
			
			context.write(new IntPair(movie_1, movie_2), new IntForthPair(rating_1, n_1,rating_2,n_2));
			
		}
	}
	
	
	
	/*-------------------------Reduce 2------------------------------------*/
	static class Reduce_2 extends TableReducer<IntPair, IntForthPair,ImmutableBytesWritable> {
		@Override
		protected void reduce(IntPair key, Iterable<IntForthPair> values, Context context) throws IOException, InterruptedException {
			int X_Y_sum = 0;
			int X_X_sum = 0;
			int Y_Y_sum = 0;
			int X_sum = 0;
			int Y_sum = 0;
			int count = 0;
			int n1 = 0;
			int n2 =0;
			
			ArrayList <Integer> rating_x_list= new ArrayList<Integer>();
			ArrayList <Integer> rating_y_list= new ArrayList<Integer>();
			ArrayList <Integer> n_x_list= new ArrayList<Integer>();
			ArrayList <Integer> n_y_list= new ArrayList<Integer>();
			for(IntForthPair pair : values){
				rating_x_list.add(pair.getFirst().get());				
				n_x_list.add(pair.getSecond().get());
				rating_y_list.add(pair.getThird().get());
				n_y_list.add(pair.getForth().get());
				//IntPair pair_temp = new IntPair(pair.getFirst().get(), pair.getSecond().get());
				//X_Y_sum += pair_temp.getFirst().get() * pair_temp.getSecond().get();
				//X_X_sum += pair_temp.getFirst().get() * pair_temp.getFirst().get();
				//Y_Y_sum += pair_temp.getSecond().get() * pair_temp.getSecond().get();
				//X_sum += pair_temp.getFirst().get();
				//Y_sum += pair_temp.getSecond().get();
				//count++;
			}
			
			n1 = n_x_list.get(0);
			n2 = n_y_list.get(0);
			for(int i=0; i<rating_x_list.size();i++){
				X_Y_sum += rating_x_list.get(i) * rating_y_list.get(i);
				X_X_sum += rating_x_list.get(i) * rating_x_list.get(i);
				Y_Y_sum += rating_y_list.get(i) * rating_y_list.get(i);
				X_sum +=rating_x_list.get(i);
				Y_sum +=rating_y_list.get(i);
				//n1 += n_x_list.get(i);
				//n2 += n_y_list.get(i);
				count++;
				//if(rating_x_list.get(i)==372784){
					//System.out.println(key.toString() + "    " + rating_x_list.get(i) +" "+ rating_y_list.get(i));
					//System.out.println(".........................");
				//}
				
				
			}
			if(count>1){
				//Cosine Similarity
				double CosSim = ((double)X_Y_sum+ 0.01)/((double)(Math.sqrt((double)X_X_sum)*Math.sqrt((double)Y_Y_sum))+0.01);
				String CosSim_string = String.valueOf(CosSim);
				
				//Correlation
				double Correlation = ((double)(count*X_Y_sum -X_sum*Y_sum)+0.0001)/((double)(Math.sqrt((double)count*X_X_sum - X_sum*X_sum) * (double)Math.sqrt((double)count*Y_Y_sum - Y_sum*Y_sum))+0.0001);
				String Correlation_string = String.valueOf(Correlation);
				
				//Jaccard Similarity
				double Jaccard = (double)(count)/(double)(n1+n2-count);
				String Jaccard_string = String.valueOf(Jaccard);
				
				String rowstr = key.toString();
				byte [] rowkey = Bytes.toBytes(rowstr);
				
				
				
				Put put = new Put(rowkey);
				put.add(Bytes.toBytes("data"),Bytes.toBytes("CosSim"),Bytes.toBytes(CosSim_string));
				put.add(Bytes.toBytes("data"),Bytes.toBytes("CorrelationSim"),Bytes.toBytes(Correlation_string));
				put.add(Bytes.toBytes("data"),Bytes.toBytes("JaccardSim"),Bytes.toBytes(Jaccard_string));

				context.write(new ImmutableBytesWritable(rowkey), put);
				//context.write(key, new DoubleWritable(sum/count));
			}
			}
			
			
			
			
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String table1 = args[0];
		String table2 = args[1];
		String outputTable = args[2]; 
		String columnFamily = args[3];
		String qualifier = args[4];
		
		System.out.println(table1 + " " +table2 +" " +outputTable + " " + columnFamily + " " +  qualifier);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		
		// we can pass column family and qualifier in conf to reducers 
		conf.set("conf.columnFamily", columnFamily);
		conf.set("conf.qualifier", qualifier);
		//Scan scan = new Scan();
		
		
		
		//MultiTableInputFormat mtic = new MultiTableInputFormat(); 
		//mtic.
		//----------------------a mapper read two hbase table  
		List<Scan> scans = new ArrayList<Scan>();
		Scan scan1 = new Scan();
		scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table1.getBytes());
		scans.add(scan1);
		
		Scan scan2 = new Scan();
		scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table2.getBytes());
		scans.add(scan2);
		//-------------------------------------------------
		
		Job job = Job.getInstance(conf);
		
		
		
		
		//mapreduce 0............................................
	    TableMapReduceUtil.initTableMapperJob(scans, Mapper_0.class,
	    		IntWritable.class, IntPair.class, job);
	    job.setReducerClass(Reduce_0.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntPair.class);
	    FileOutputFormat.setOutputPath(job, new Path(TMP_DIR + "/output0/"));	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntTriple.class);
	    //job.setNumReduceTasks(0);
	    job.setJarByClass(ExtractMovieSimilar.class);
	    job.waitForCompletion(true);
	    //....................................................
		
	    
		//mapreduce 1............................................
	    Job job1 = Job.getInstance(conf);
	    job1.setMapperClass(Mapper_1.class);
	    job1.setReducerClass(Reduce_1.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    job1.setInputFormatClass(TextInputFormat.class);
	    job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(IntTriple.class);
	    FileInputFormat.setInputPaths(job1, new Path(TMP_DIR + "/output0/"));	
	    FileOutputFormat.setOutputPath(job1, new Path(TMP_DIR + "/output1/"));	    
	    job1.setOutputKeyClass(IntPair.class);
	    job1.setOutputValueClass(IntForthPair.class);
	    //job.setNumReduceTasks(0);
	    job1.setJarByClass(ExtractMovieSimilar.class);
	    job1.waitForCompletion(true);
	    //....................................................
	    
	    
	    
	    
	    
	    
	    Job job2 = Job.getInstance(conf);
	    job2.setMapperClass(Mapper_2.class);
	    TableMapReduceUtil.initTableReducerJob(outputTable,  Reduce_2.class, job2);
	    job2.setInputFormatClass(TextInputFormat.class);
	//    job2.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job2, new Path(TMP_DIR + "/output1/"));	
	//    FileOutputFormat.setOutputPath(job2, new Path(TMP_DIR + "/output2/"));
	    job2.setMapOutputKeyClass(IntPair.class);
	    job2.setMapOutputValueClass(IntForthPair.class);
	   
	  //  job2.setOutputKeyClass(IntPair.class);
	 //   job2.setOutputValueClass(IntPair.class);
	 //   job2.setNumReduceTasks(0);
	    job2.setJarByClass(ExtractMovieSimilar.class);
	    job2.waitForCompletion(true);
	    
	    
	    
		
	}
}

