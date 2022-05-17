package Bigdata.hadoop.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AnalysisBatchView {

    private static String getDay(long timeStamp) {
        timeStamp *= 1000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(":yyyyMMdd");
        return simpleDateFormat.format(timeStamp);
    }

    private static long getDayTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(":yyyyMMdd");
        return simpleDateFormat.parse(Day).getTime();
    }

    private static long getMinTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        return simpleDateFormat.parse(Day).getTime();
    }

    private static String getMin(Long timeStamp) {
        String pattern = "yyyyMMddHHmm";
        timeStamp *= 1000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.format(new Date(timeStamp));
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            try {
                JSONObject item = new JSONObject(text.toString());
                StringBuilder result = new StringBuilder();
                JSONObject Disk = item.getJSONObject("Disk");
                JSONObject Ram = item.getJSONObject("RAM");
                result.append(item.getDouble("CPU")).append(",").append(Disk.getDouble("Free") / Disk.getDouble("Total"))
                        .append(",").append(Ram.getDouble("Free") / Ram.getDouble("Total"))
                        .append(",").append(item.getLong("Timestamp"));
                value.set(result.toString());
                key.set(getDay(item.getLong("Timestamp")) + "," + item.getString("serviceName"));
                context.write(key, value);
                key.set(getMin(item.getLong("Timestamp")) + "," + item.getString("serviceName"));
                context.write(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        final String DELIMITER = ",";
        private final Text value = new Text();
        private MultipleOutputs<Text,Text> mos;
        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            setValue(values);
            try {
                if (k.charAt(0) == 58) {
                    key.set(getDayTimeStamp(k.substring(0, 9)) + k.substring(9));
                    mos.write("Day", key, value,"Day/Day");
                } else {
                    key.set(getMinTimeStamp(k.substring(0, 13)) + k.substring(13));
                    mos.write("Min", key, value,"Min/Min");
                }
            } catch (ParseException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        private void setValue(Iterable<Text> values) {
            double CPU = 0, Disk = 0, Ram = 0, temp, peakCpu = 0, peakDisk = 0, peakRam = 0;
            long tPeakCpu = 0, tPeakDisk = 0, tPeakRam = 0;
            int count = 0;
            String[] data;
            for (Text v : values) {
                data = v.toString().split(DELIMITER);
                temp = Double.parseDouble(data[0]);
                CPU += temp;
                if (peakCpu < temp) {
                    peakCpu = temp;
                    tPeakCpu = Long.parseLong(data[3]);
                }
                temp = Double.parseDouble(data[1]);
                Disk += temp;
                if (peakDisk < temp) {
                    peakDisk = temp;
                    tPeakDisk = Long.parseLong(data[3]);
                }
                temp = Double.parseDouble(data[2]);
                Ram += temp;
                if (peakRam < temp) {
                    peakRam = temp;
                    tPeakRam = Long.parseLong(data[3]);
                }
                count++;
            }
            value.set(count + "," + CPU / count + "," + peakCpu + "," + tPeakCpu + "," + Disk / count + "," + peakDisk + "," + tPeakDisk + "," + Ram / count + "," + peakRam + "," + tPeakRam);
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Analysis Batch");
        job.setJarByClass(AnalysisBatchView.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/test"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/Analysis"));
        MultipleOutputs.addNamedOutput(job, "Day", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Min", TextOutputFormat.class, Text.class, Text.class);
        job.waitForCompletion(true);
    }
}
