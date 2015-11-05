package org.CMPT732A3;

import java.io.IOException;
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

public class ExtractRatingCount {
    private final static IntWritable one = new IntWritable(1);

    static class RatingMapper extends
            TableMapper<ImmutableBytesWritable, IntWritable> {
        private byte[] columnFamily;
        private byte[] qualifier;

        @Override
        protected void setup(Context context) {
            // get column family and qualifer from context.getConfiguration()
            Configuration conf = context.getConfiguration();
            columnFamily = Bytes.toBytes(conf.get("conf.columnFamily", null));
            qualifier = Bytes.toBytes(conf.get("conf.qualifier", null));
        }

        @Override
        public void map(ImmutableBytesWritable row, Result value,
                Context context) throws IOException, InterruptedException {
            byte[] b = row.get();
            String s = new String(b, "UTF-8");
            String movieID_string = s.split(":")[0];
            byte[] movierowkey = Bytes.toBytes(movieID_string);

            byte[] rating_byte = value.getValue(columnFamily, qualifier);
            int rating = Integer.parseInt(new String(rating_byte, "UTF-8"));
            if (rating > 0) {
                context.write(new ImmutableBytesWritable(movierowkey), one);
            }

        }
    }

    // reducer will take movieID as key and ratings as values, will generate
    // <movieID, averageRating>
    static class RatingTableReducer
            extends
            TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key,
                Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            String count = Integer.toString(sum);
            Put put = new Put(key.get());
            put.add(Bytes.toBytes("data"), Bytes.toBytes("Count"),
                    Bytes.toBytes(count));
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        String table = args[0];
        String outputTable = args[1];
        String columnFamily = args[2];
        String qualifier = args[3];

        System.out.println(table + " " + outputTable + " " + columnFamily + " "
                + qualifier);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        // we can pass column family and qualifier in conf to reducers
        conf.set("conf.columnFamily", columnFamily);
        conf.set("conf.qualifier", qualifier);
        Scan scan = new Scan();

        Job job = Job.getInstance(conf);

        TableMapReduceUtil.initTableMapperJob(table, scan, RatingMapper.class,
                ImmutableBytesWritable.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable,
                RatingTableReducer.class, job);

        // job.setReducerClass(RatingReduce.class);
        // FileOutputFormat.setOutputPath(job, new Path(outputDir));

        // job.setOutputKeyClass(IntWritable.class);
        // job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(ExtractRatingCount.class);
        job.waitForCompletion(true);

    }
}
