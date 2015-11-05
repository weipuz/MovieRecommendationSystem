package org.CMPT732A3;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.CMPT732A3.ExtractRatingCount.RatingMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
    static private final Path TMP_DIR = new Path(
            UserMovieDependency.class.getSimpleName() + "_TMP_");
    private final static IntWritable one = new IntWritable(1);

    /*-------------------------Mapper 1------------------------------------*/
    static class UserMapper_1 extends TableMapper<Text, Text> {
        private byte[] columnFamily;
        private byte[] qualifier_genres;

        @Override
        protected void setup(Context context) {
            // get column family and qualifer from context.getConfiguration()
            Configuration conf = context.getConfiguration();
            columnFamily = Bytes.toBytes(conf.get("conf.columnFamily", "data"));
            qualifier_genres = Bytes.toBytes(conf.get(
                    "conf.qualifier_occupation", "Genres"));
        }

        @Override
        public void map(ImmutableBytesWritable row, Result value,
                Context context) throws IOException, InterruptedException {
            byte[] b = row.get();
            String s = new String(b, "UTF-8");
            String[] row_array = s.split(":");
            String movierowkey = row_array[0];
            if (row_array.length == 1) {
                byte[] genres = value.getValue(columnFamily, qualifier_genres);
                if (genres != null) {
                    String genres_string = new String(genres, "UTF-8");
                    context.write(new Text(movierowkey), new Text("genres:"
                            + genres_string));
                }
            } else {
                byte[] userID = Bytes.toBytes(row_array[1]);
                String userID_string = new String(userID, "UTF-8");
                context.write(new Text(movierowkey), new Text("user:"
                        + userID_string));
            }
        }
    }

    /*-------------------------Reduce 1------------------------------------*/
    // reducer1 will take movieID as key and a pair of info as values, will
    // generate (movieID:userID, Genres) to a TMP Hbase

    static class UserReducer_1 extends
            TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres = null;
            List<String> users = new ArrayList<String>();
            for (Text value : values) {
                String[] data = value.toString().split(":");
                if (data.length != 2) {
                    continue;
                }
                String type = data[0];
                if (type.equals("genres")) {
                    genres = data[1];
                } else if (type.equals("user")) {
                    users.add(data[1]);
                }
            }

            if (genres == null || users.isEmpty()) {
                return;
            }

            for (String user : users) {
                byte[] rowkey = Bytes.toBytes(key.toString() + ":" + user); 
                // movieID+:+userID as row key;
                Put put = new Put(rowkey);
                put.add(Bytes.toBytes("data"), Bytes.toBytes("Genres"),
                        Bytes.toBytes(genres));
                context.write(new ImmutableBytesWritable(rowkey), put);
            }

        }
    }

    /*-------------------------Mapper 2------------------------------------*/
    // Mapper 2 will join temp table and the user info table on the userID
    static class UserMapper_2 extends TableMapper<Text, Text> {
        private byte[] columnFamily;
        private byte[] qualifier;
        private byte[] qualifier_age;
        private byte[] qualifier_gender;
        private byte[] qualifier_occupation;
        private byte[] qualifier_genres;

        @Override
        protected void setup(Context context) {
            // get column family and qualifer from context.getConfiguration()
            Configuration conf = context.getConfiguration();
            columnFamily = Bytes.toBytes(conf.get("conf.columnFamily", "data"));
            qualifier_age = Bytes
                    .toBytes(conf.get("conf.qualifier_age", "Age"));
            qualifier_gender = Bytes.toBytes(conf.get("conf.qualifier_gender",
                    "Gender"));
            qualifier_occupation = Bytes.toBytes(conf.get(
                    "conf.qualifier_occupation", "Occupation"));
            qualifier_genres = Bytes.toBytes(conf.get(
                    "conf.qualifier_occupation", "Genres"));

        }

        @Override
        public void map(ImmutableBytesWritable row, Result value,
                Context context) throws IOException, InterruptedException {

            byte[] b = row.get();
            String s = new String(b, "UTF-8");
            String[] row_array = s.split(":");

            if (row_array.length == 1) {
                String userId = row_array[0];
                byte[] age = value.getValue(columnFamily, qualifier_age);
                byte[] gender = value.getValue(columnFamily, qualifier_gender);
                byte[] occupation = value.getValue(columnFamily,
                        qualifier_occupation);

                if (age != null) {
                    String age_string = new String(age, "UTF-8");
                    System.out.println(age_string);
                    context.write(new Text(userId), new Text("age:"
                            + age_string));
                }
                if (gender != null) {
                    String gender_string = new String(gender, "UTF-8");
                    context.write(new Text(userId), new Text("gender:"
                            + gender_string));
                }
                if (occupation != null) {
                    String occupation_string = new String(occupation, "UTF-8");
                    context.write(new Text(userId), new Text("occupation:"
                            + occupation_string));
                }
            }

            else if (row_array.length == 2) {
                String movieId = row_array[0];
                String userId = row_array[1];
                byte[] genres = value.getValue(columnFamily, qualifier_genres);
                if (genres != null) {
                    String genres_string = new String(genres, "UTF-8");
                    context.write(new Text(userId), new Text("movie:" + movieId
                            + ":genres:" + genres_string));
                }
            }
        }
    }

    /*-------------------------Reduce 2------------------------------------*/
    // reducer2 will take userID as key and text as info as values, will write a
    // row to a Hbase table in the follow format;
    // rowkey: <movieID:userID>, column family: data, column qualifiers: <Age,
    // Gender, Occupation, Genres>;

    static class UserReducer_2 extends
            TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String age = null;
            String gender = null;
            String occupation = null;
            List<String> movieinfo = new ArrayList<String>();

            for (Text value : values) {
                String[] data = value.toString().split(":");
                if (data.length < 2) {
                    continue;
                }
                String type = data[0];
                if (type.equals("age")) {
                    age = data[1];
                }
                if (type.equals("gender")) {
                    gender = data[1];
                }
                if (type.equals("occupation")) {
                    occupation = data[1];
                } else if (type.equals("movie")) {
                    movieinfo.add(data[1] + ":" + data[3]);
                }
            }

            if (movieinfo.isEmpty() || age == null || gender == null
                    || occupation == null) {
                return;
            }

            for (String movie : movieinfo) {
                String movieID = movie.split(":")[0];
                String genres = movie.split(":")[1];
                byte[] rowkey = Bytes.toBytes(movieID + ":" + key.toString()); 
                // movieID+:+userID as row key;
                Put put = new Put(rowkey);
                put.add(Bytes.toBytes("data"), Bytes.toBytes("Age"),
                        Bytes.toBytes(age));
                put.add(Bytes.toBytes("data"), Bytes.toBytes("Gender"),
                        Bytes.toBytes(gender));
                put.add(Bytes.toBytes("data"), Bytes.toBytes("Occupation"),
                        Bytes.toBytes(occupation));
                put.add(Bytes.toBytes("data"), Bytes.toBytes("Genres"),
                        Bytes.toBytes(genres));
                context.write(new ImmutableBytesWritable(rowkey), put);
            }

        }
    }

    /*-------------------------Mapper 3------------------------------------*/
    // Mapper 2 will join temp table and the user info table on the userID
    static class UserMapper_3 extends TableMapper<Text, IntWritable> {
        private byte[] columnFamily;
        private byte[] qualifier_age;
        private byte[] qualifier_gender;
        private byte[] qualifier_occupation;
        private byte[] qualifier_genres;

        @Override
        protected void setup(Context context) {
            // get column family and qualifer from context.getConfiguration()
            Configuration conf = context.getConfiguration();
            columnFamily = Bytes.toBytes(conf.get("conf.columnFamily", "data"));
            qualifier_age = Bytes
                    .toBytes(conf.get("conf.qualifier_age", "Age"));
            qualifier_gender = Bytes.toBytes(conf.get("conf.qualifier_gender",
                    "Gender"));
            qualifier_occupation = Bytes.toBytes(conf.get(
                    "conf.qualifier_occupation", "Occupation"));
            qualifier_genres = Bytes.toBytes(conf.get("conf.qualifier_genres",
                    "Genres"));

        }

        @Override
        public void map(ImmutableBytesWritable row, Result value,
                Context context) throws IOException, InterruptedException {

            byte[] b = row.get();
            String s = new String(b, "UTF-8");
            String[] row_array = s.split(":");

            String userId = row_array[0];
            byte[] age = value.getValue(columnFamily, qualifier_age);
            byte[] gender = value.getValue(columnFamily, qualifier_gender);
            byte[] occupation = value.getValue(columnFamily,
                    qualifier_occupation);
            byte[] genres = value.getValue(columnFamily, qualifier_genres);

            if (age == null || gender == null || occupation == null
                    || genres == null) {
                return;
            }
            String age_string = new String(age, "UTF-8");
            String gender_string = new String(gender, "UTF-8");
            String occupation_string = new String(occupation, "UTF-8");
            String genres_string = new String(genres, "UTF-8");
            // System.out.println(genres_string);
            String[] genres_array = genres_string.split("\\|");

            if (genres_array != null && genres_array.length >= 1) {
                for (int i = 0; i < genres_array.length; i++) {
                    String genre = genres_array[i];
                    context.write(new Text("age:" + age_string + ":" + genre),
                            one);
                    context.write(new Text("gender:" + gender_string + ":"
                            + genre), one);
                    context.write(new Text("occupation:" + occupation_string
                            + ":" + genre), one);
                }

            }

        }
    }

    /*-------------------------Reduce 3------------------------------------*/
    // reducer3 will take input from temptable_2 and count occurence of each
    // userGroup and Genre pair;
    // input: rowkey: <movieID:userID>, column family: data, column qualifiers:
    // <Age, Gender, Occupation, Genres>;
    // output: (<Usergroup,Genres>,count)

    static class UserReducer_3 extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static final Map<String, String> occpuations = new HashMap<String, String>();
    static {

        occpuations.put("0", "other");
        occpuations.put("1", "academic/educator");
        occpuations.put("2", "artist");
        occpuations.put("3", "clerical/admin");
        occpuations.put("4", "college/grad student");
        occpuations.put("5", "customer service");
        occpuations.put("6", "doctor/health care");
        occpuations.put("7", "executive/managerial");
        occpuations.put("8", "farmer");
        occpuations.put("9", "homemaker");
        occpuations.put("10", "K-12 student");
        occpuations.put("11", "lawyer");
        occpuations.put("12", "programmer");
        occpuations.put("13", "retired");
        occpuations.put("14", "sales/marketing");
        occpuations.put("15", "scientist");
        occpuations.put("16", "self-employed");
        occpuations.put("17", "technician/engineer");
        occpuations.put("18", "tradesman/craftsman");
        occpuations.put("19", "unemployed");
        occpuations.put("20", "writer");

    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        String movieTable = "weipuz_movieInfo_1M";
        String ratingTable = "weipuz_ratingInfo_1M";
        String tempTable = "weipuz_temp";
        String userTable = "weipuz_userInfo";
        String tempTable_2 = "weipuz_temp_2";
        String columnFamily = "data";
        String qualifier_genres = "Genres";
        String qualifier_age = "Age";
        String qualifier_gender = "Gender";
        String qualifier_occupation = "Occupation";

        System.out.println(movieTable + " " + ratingTable + " " + tempTable
                + " " + tempTable_2);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        // we can pass column family and qualifier in conf to mapreduce jobs;
        conf.set("conf.columnFamily", columnFamily);
        conf.set("conf.qualifier_genres", qualifier_genres);
        conf.set("conf.qualifier_age", qualifier_age);
        conf.set("conf.qualifier_gender", qualifier_gender);
        conf.set("conf.qualifier_occupation", qualifier_occupation);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan1 = new Scan();
        scan1.setAttribute("scan.attributes.table.name",
                Bytes.toBytes(movieTable));
        System.out.println(scan1.getAttribute("scan.attributes.table.name"));
        scans.add(scan1);
        Scan scan2 = new Scan();
        scan2.setAttribute("scan.attributes.table.name",
                Bytes.toBytes(ratingTable));
        System.out.println(scan2.getAttribute("scan.attributes.table.name"));
        scans.add(scan2);

        Job job = Job.getInstance(conf);
        TableMapReduceUtil.initTableMapperJob(scans, UserMapper_1.class,
                Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(tempTable, UserReducer_1.class,
                job);
        job.setJarByClass(ExtractRatingCount.class);
        job.waitForCompletion(true);

        List<Scan> scans2 = new ArrayList<Scan>();
        scan1.setAttribute("scan.attributes.table.name",
                Bytes.toBytes(tempTable));
        System.out.println(scan1.getAttribute("scan.attributes.table.name"));
        scans2.add(scan1);
        scan2.setAttribute("scan.attributes.table.name",
                Bytes.toBytes(userTable));
        System.out.println(scan2.getAttribute("scan.attributes.table.name"));
        scans2.add(scan2);

        Job job2 = Job.getInstance(conf);
        TableMapReduceUtil.initTableMapperJob(scans2, UserMapper_2.class,
                Text.class, Text.class, job2);
        TableMapReduceUtil.initTableReducerJob(tempTable_2,
                UserReducer_2.class, job2);
        job2.setJarByClass(ExtractRatingCount.class);
        job2.waitForCompletion(true);

        Scan scan = new Scan();
        Job job3 = Job.getInstance(conf);
        TableMapReduceUtil.initTableMapperJob(tempTable_2, scan,
                UserMapper_3.class, Text.class, IntWritable.class, job3);
        job3.setReducerClass(UserReducer_3.class);
        FileOutputFormat.setOutputPath(job3, new Path(TMP_DIR + "/output/"));
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setJarByClass(ExtractRatingCount.class);
        job3.waitForCompletion(true);

        SequenceFile.Reader reader = new SequenceFile.Reader(
                job3.getConfiguration(), SequenceFile.Reader.file(new Path(
                        TMP_DIR + "/output/part-r-00000")));
        Text data = new Text();
        IntWritable count = new IntWritable();

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(
                    "AgeDependency.csv"));
            out.write("AgeGroup,Genre,Problity,\n");
            String type = null;
            String group = null;
            String gerne = null;
            Map<String, Integer> map = new HashMap<String, Integer>();
            int sum = 0;
            while (reader.next(data, count)) {
                String[] data_array = data.toString().split(":");
                // System.out.println(data);
                if (type == null || group == null || gerne == null) {
                    // first line;
                    type = data_array[0];
                    group = data_array[1];
                    gerne = data_array[2];
                    map.put(gerne, count.get());
                    sum = count.get();
                    continue;
                } else if (data_array != null && type.equals(data_array[0])
                        && group.equals(data_array[1])) {
                    // keep reading infos of same type and group and put into
                    // map;
                    gerne = data_array[2];
                    map.put(gerne, count.get());
                    sum += count.get();
                } else if (data_array != null && type.equals(data_array[0])) {

                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        double p = (double) entry.getValue() / (double) sum;
                        out.write(group + "," + entry.getKey() + "," + p
                                + ",\n");
                        System.out.println(group + "," + entry.getKey() + ","
                                + p + ",");
                    }
                    map.clear();
                    type = data_array[0];
                    group = data_array[1];
                    gerne = data_array[2];
                    map.put(gerne, count.get());
                    sum = count.get();
                } else if (data_array != null && data_array[0].equals("gender")) {
                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        double p = (double) entry.getValue() / (double) sum;
                        out.write(group + "," + entry.getKey() + "," + p
                                + ",\n");
                        System.out.println(group + "," + entry.getKey() + ","
                                + p + ",");
                    }
                    map.clear();
                    type = data_array[0];
                    group = data_array[1];
                    gerne = data_array[2];
                    map.put(gerne, count.get());
                    sum = count.get();

                    out.close();
                    out = new BufferedWriter(new FileWriter(
                            "GenderDependency.csv"));
                    out.write("GenderGroup,Genre,Problity,\n");

                } else if (data_array != null
                        && data_array[0].equals("occupation")) {
                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        double p = (double) entry.getValue() / (double) sum;
                        out.write(group + "," + entry.getKey() + "," + p
                                + ",\n");
                        System.out.println(group + "," + entry.getKey() + ","
                                + p + ",");
                    }
                    map.clear();
                    type = data_array[0];
                    group = data_array[1];
                    gerne = data_array[2];
                    map.put(gerne, count.get());
                    sum = count.get();

                    out.close();
                    out = new BufferedWriter(new FileWriter(
                            "OccuptionDependency.csv"));
                    out.write("Occupation,Genre,Problity,\n");
                }

            }
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                double p = (double) entry.getValue() / (double) sum;
                out.write(group + "," + entry.getKey() + "," + p + ",\n");
                System.out
                        .println(group + "," + entry.getKey() + "," + p + ",");
            }
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
