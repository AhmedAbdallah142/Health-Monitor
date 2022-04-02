package Bigdata.hadoop.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class BetweenDate {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long from = Long.parseLong(conf.get("from")), to = Long.parseLong(conf.get("to"));
            String[] textSplit = text.toString().split("\t");
            String[] keySplit = textSplit[0].split(",");
            try {
                long time = getTimeStamp(keySplit[0] + " 01");
                if (time >= from && time <= to) {
                    key.set(keySplit[1]);
                    value.set(textSplit[1]);
                    context.write(key, value);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] results;
            double CPU = 0, Disk = 0, Ram = 0, temp;
            double peakCpu = 0, peakDisk = 0, peakRam = 0;
            long tPeakCpu = 0, tPeakDisk = 0, tPeakRam = 0;
            int count = 0;
            for (Text t:values){
                results = t.toString().split(",");

            }
            context.write(key, value);
        }
    }

    private static String getDate(Long timeStamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
        return simpleDateFormat.format(new Date(timeStamp));
    }

    private static long getTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy hh");
        return simpleDateFormat.parse(Day).getTime();
    }

    public static void analyze(String fromDay, String toDay) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("from", getTimeStamp(fromDay + " 00"));
        conf.setLong("to", getTimeStamp(toDay + " 24"));

        Job job = Job.getInstance(conf, "BetweenDate");
        job.setJarByClass(BetweenDate.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("analysis"));
        FileOutputFormat.setOutputPath(job, new Path("result"));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        analyze("02-04-2022","02-04-2022");
//        System.out.println(getTimeStamp("02-04-2022 00"));
//        System.out.println(getTimeStamp("02-04-2022 01"));
//        System.out.println(getTimeStamp("02-04-2022 02"));
//        System.out.println(getTimeStamp("02-04-2022 10"));
//        System.out.println(getTimeStamp("02-04-2022 11"));
//        Date d = new Date(getTimeStamp("02-04-2022 24"));
//        System.out.println(d);
    }
}
