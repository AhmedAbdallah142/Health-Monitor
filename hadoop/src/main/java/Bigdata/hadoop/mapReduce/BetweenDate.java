package Bigdata.hadoop.mapReduce;

import Bigdata.monitor.FileOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class BetweenDate {
    static FileOperation file = new FileOperation();
    final static String ResultPath = "hdfs://localhost:9000/Results";
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
                if (time >= from && time < to) {
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
            int count = 0, tempCount;
            for (Text t : values) {
                results = t.toString().split(",");
                tempCount = Integer.parseInt(results[0]);
                CPU += Double.parseDouble(results[1]) * tempCount;
                temp = Double.parseDouble(results[2]);
                if (peakCpu < temp) {
                    peakCpu = temp;
                    tPeakCpu = Long.parseLong(results[3]);
                }
                Disk += Double.parseDouble(results[4]) * tempCount;
                temp = Double.parseDouble(results[5]);
                if (peakDisk < temp) {
                    peakDisk = temp;
                    tPeakDisk = Long.parseLong(results[6]);
                }
                Ram += Double.parseDouble(results[7]) * tempCount;
                temp = Double.parseDouble(results[8]);
                if (peakRam < temp) {
                    peakRam = temp;
                    tPeakRam = Long.parseLong(results[9]);
                }
                count += tempCount;
            }
            value.set(count + "," + CPU / count + "," + tPeakCpu + "," + Disk / count + "," + tPeakDisk + "," + Ram / count + "," + tPeakRam);
            context.write(key, value);
            System.out.println("\n\n\n" + value);
        }
    }

    private static long getTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy hh");
        return simpleDateFormat.parse(Day).getTime();
    }

    public static String analyze(String fromDay, String toDay) throws Exception {
        FileSystem fileSystem = file.configureFileSystem();
        file.DeleteFile(fileSystem, ResultPath);
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

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Analysis"));
        FileOutputFormat.setOutputPath(job, new Path(ResultPath));
        job.waitForCompletion(true);
        return getResults(fileSystem);
    }

    public static String getResults(FileSystem fileSystem) throws IOException {
        String result = file.ReadFile(fileSystem, ResultPath + "/part-r-00000");
        file.DeleteFile(fileSystem, ResultPath);
        file.closeFileSystem(fileSystem);
        return result;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        analyze("01-07-2022", "01-10-2022");
    }
}
