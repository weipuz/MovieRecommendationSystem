package org.CMPT732A3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class ExtractUserInfo {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        Configuration config = HBaseConfiguration.create();
        // Create table
        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor htd = new HTableDescriptor(
                TableName.valueOf("weipuz_userInfo"));
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
        String filename = "users.dat";
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        int row_index = 0;
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
            int id = Integer.valueOf(aline[0]);
            byte[] row1 = Bytes.toBytes(String.format("%04d", id));
            Put p1 = new Put(row1);
            byte[] databytes = Bytes.toBytes("data");
            p1.add(databytes, Bytes.toBytes("Gender"), Bytes.toBytes(aline[1]));

            // put the third element into table;
            if (aline.length >= 3) {
                p1.add(databytes, Bytes.toBytes("Age"), Bytes.toBytes(aline[2]));
            }
            if (aline.length >= 4) {
                p1.add(databytes, Bytes.toBytes("Occupation"),
                        Bytes.toBytes(aline[3]));
            }
            if (aline.length >= 5) {
                p1.add(databytes, Bytes.toBytes("Zipcode"),
                        Bytes.toBytes(aline[4]));
            }
            table.put(p1);

        }
        br.close();

        // Drop the table
        // admin.disableTable(tablename);
        // admin.deleteTable(tablename);

    }

}
