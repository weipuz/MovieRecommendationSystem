package org.CMPT732A3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class ExtractRatingInfo {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String ratingTable = args[0];// movieTable = "weipuz_ratingInfo" or
                                     // "weipuz_ratingInfo_1M";
        String input = args[1]; // String filename = "ratings.dat"; or
                                // "1M/ratings.dat";
        Configuration config = HBaseConfiguration.create();
        // Create table
        HBaseAdmin admin = new HBaseAdmin(config);
        long starttime = System.currentTimeMillis();

        HTableDescriptor htd = new HTableDescriptor(
                TableName.valueOf(ratingTable));
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
            if (aline.length < 4) {
                System.out.println(line);
                continue;
            }
            if (count == 100) {
                table.flushCommits();
                count = 0;
            }

            byte[] row1 = Bytes.toBytes(aline[1] + ":" + aline[0]); // movieID +
                                                                    // : +
                                                                    // userID as
                                                                    // row key;
            Put p1 = new Put(row1);
            byte[] databytes = Bytes.toBytes("data");
            p1.add(databytes, Bytes.toBytes("Rating"), Bytes.toBytes(aline[2]));
            table.put(p1);
            count++;

        }
        br.close();
        table.flushCommits();
        long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println("time used: " + time / 1000 + " seconds");
        // Drop the table
        // admin.disableTable(tablename);
        // admin.deleteTable(tablename);

    }

}
