package org.CMPT732A3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MovieClient {
	
	private static Configuration conf = null;
	
	private static ArrayList <Double> Cosine_list= new ArrayList<Double>();
	private static ArrayList <Double> Correlation_list= new ArrayList<Double>();
	private static ArrayList <Double> Jaccard_list= new ArrayList<Double>();
	private static ArrayList <String> Movie_list= new ArrayList<String>();
	
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }
 
    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }
 
    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }
 
    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        List<Cell> cellsc = rs.listCells();
        for(Cell cell : cellsc){
            System.out.print(new String(CellUtil.cloneRow(cell)) + " " );
            System.out.print(new String(CellUtil.cloneFamily(cell)) + ":" );
            System.out.print(new String(CellUtil.cloneQualifier(cell)) + " " );
            //System.out.print(cell.getTimestamp() + " " );
            System.out.println(new String(CellUtil.cloneValue(cell)));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r:ss){
            	 List<Cell> cellsc = r.listCells();
                 for(Cell cell : cellsc){
                     System.out.print(new String(CellUtil.cloneRow(cell)) + " " );
                     System.out.print(new String(CellUtil.cloneFamily(cell)) + ":" );
                     System.out.print(new String(CellUtil.cloneQualifier(cell)) + " " );
                     //System.out.print(cell.getTimestamp() + " " );
                     System.out.println(new String(CellUtil.cloneValue(cell)));
                 }
             }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    public static String findMovieID (String tableName,String movieName){
    	String result = null;
    	try{
    		
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
           	 List<Cell> cellsc = r.listCells();
                for(Cell cell : cellsc){
                	if(new String(CellUtil.cloneValue(cell)).equals(movieName)){
                		result= (new String(CellUtil.cloneRow(cell)));
                	}
                    //System.out.print(new String(CellUtil.cloneRow(cell)) + " " );
                    //System.out.print(new String(CellUtil.cloneFamily(cell)) + ":" );
                    //System.out.print(new String(CellUtil.cloneQualifier(cell)) + " " );
                    //System.out.print(cell.getTimestamp() + " " );
                    //System.out.println(new String(CellUtil.cloneValue(cell)));
                }
            }
       } catch (IOException e){
           e.printStackTrace();
       }
		return result;
    }
    
    public static void getSim (String tableName, String movieID){
    	try{
    		
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
           	 List<Cell> cellsc = r.listCells();
                for(Cell cell : cellsc){
                	String rowkey = new String(CellUtil.cloneRow(cell));
                	String [] rowkey_part = rowkey.split(":");
                	String rowkey_part1 = rowkey_part[0];
                	String rowkey_part2 = rowkey_part[1];
                	String qualifier = new String(CellUtil.cloneQualifier(cell));
                	if(rowkey_part1.equals(movieID) && qualifier.equals("CorrelationSim")){
                		Correlation_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                	}
                	if(rowkey_part2.equals(movieID) && qualifier.equals("CorrelationSim")){
                		Correlation_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                	}
                	
                	
                	
                	if(rowkey_part1.equals(movieID) && qualifier.equals("CosSim")){
                		Cosine_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                	}
                	if(rowkey_part2.equals(movieID) && qualifier.equals("CosSim")){
                		Cosine_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                	}
                	
                	
                	
                	if(rowkey_part1.equals(movieID) && qualifier.equals("JaccardSim")){
                		Jaccard_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                		Movie_list.add(rowkey_part2);
                		//System.out.println(Movie_list.add(rowkey_part2));
                		//System.out.println("................");
                	}
                	if(rowkey_part2.equals(movieID) && qualifier.equals("JaccardSim")){
                		Jaccard_list.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                		Movie_list.add(rowkey_part1);
                	}
                	//if(new String(CellUtil.cloneValue(cell)).equals(movieName)){
                		//result= (new String(CellUtil.cloneRow(cell)));
                	//}
                    //System.out.print(new String(CellUtil.cloneRow(cell)) + " " );
                    //System.out.print(new String(CellUtil.cloneFamily(cell)) + ":" );
                    //System.out.print(new String(CellUtil.cloneQualifier(cell)) + " " );
                    //System.out.print(cell.getTimestamp() + " " );
                    //System.out.println(new String(CellUtil.cloneValue(cell)));
                }
            }
       } catch (IOException e){
           e.printStackTrace();
       }
    }
    
    public static void getTop10_Jaccard(ArrayList <Double> alist,ArrayList <String> movielist){
    	if(movielist.size()<=10){
    		int size = movielist.size();
    		int loop =0;
    		while(loop<size){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    		}
    	}else{
    		int loop =0;
    		while(loop<10){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    			
    		}
    		
    	}
    }
    
    public static void getTop10_Correlation(ArrayList <Double> alist,ArrayList <String> movielist){
    	if(movielist.size()<=10){
    		int size = movielist.size();
    		int loop =0;
    		while(loop<size){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    		}
    	}else{
    		int loop =0;
    		while(loop<10){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    			
    		}
    		
    	}
    }
    
    public static void getTop10_Cosine(ArrayList <Double> alist,ArrayList <String> movielist){
    	if(movielist.size()<=10){
    		int size = movielist.size();
    		int loop =0;
    		while(loop<size){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    		}
    	}else{
    		int loop =0;
    		while(loop<10){
    			int index = getBest(alist,movielist);
    			alist.remove(index);
    			//Corlist.remove(index);
    			//Jarlist.remove(index);
    			movielist.remove(index);
    			//getTop10(ratinglist,movielist);
    			loop++;
    			
    		}
    		
    	}
    }
    
    public static int getBest(ArrayList <Double> alist,ArrayList <String> movielist){
    	double max =0.0;
    	int index =0;
    	//int loop =0;
    	
		for(int i=0; i<alist.size(); i++){
    		double temp = alist.get(i);
    		if(temp>max){
    			max = temp;
    			index = i;
    		}
    	}
    	System.out.println("movie id  is " + movielist.get(index) +"     " + "rating is " + alist.get(index));
    	
    	
    	//movielist.remove(index);
    	return index;
    	//getBest(ratinglist,movielist);
    	
    	
    }
    
	public static void main(String[] args) throws IOException {
		try{
			String movieTable = "weipuz_movieInfo_1M"; 
			String simTable = "weipuz_sim_1M";
			String movieID = MovieClient.findMovieID(movieTable,"Pocahontas (1995)");
			int movieID_int = Integer.parseInt(movieID);
			//System.out.println(movieID_int);
			MovieClient.getSim(simTable, Integer.toString(movieID_int));
			
			
			ArrayList <String> movieID_list= new ArrayList<String>();
			ArrayList <String> movieID_list1= new ArrayList<String>();
			ArrayList <String> movieID_list2= new ArrayList<String>();
			
			System.out.println("The Jaccard top 10 is:  ");
			for(int i=0; i<Movie_list.size();i++){
				movieID_list.add(Movie_list.get(i));
			}
			MovieClient.getTop10_Jaccard(Jaccard_list, movieID_list);
			System.out.println("----------------------");
			
			System.out.println("The Cosine top 10 is:  ");
			for(int i=0; i<Movie_list.size();i++){
				movieID_list1.add(Movie_list.get(i));
			}
			MovieClient.getTop10_Cosine(Cosine_list, movieID_list1);
			System.out.println("----------------------");
			
			System.out.println("The Correlation top 10 is:  ");
			for(int i=0; i<Movie_list.size();i++){
				movieID_list2.add(Movie_list.get(i));
			}
			MovieClient.getTop10_Correlation(Correlation_list, movieID_list2);
			
			//System.out.println("getSim finished...............");
			//ArrayList <Double> Cosine = MovieClient.Cosine_list;
			//for(int i=0; i<Jaccard_list.size();i++){
				//System.out.println(Jaccard_list.get(i));
				//System.out.println(Movie_list.size());
				//System.out.println(movieID_list.size());
			//} 
			//System.out.println("..........................");
			
			
		}catch(Exception e) {
            e.printStackTrace();
        }
	}
}