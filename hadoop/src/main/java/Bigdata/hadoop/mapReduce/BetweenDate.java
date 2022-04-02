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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BetweenDate {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("from"));

            JSONObject item = new JSONObject(text.toString());
            StringBuilder result = new StringBuilder();

            key.set(getDate(item.getLong("Timestamp")) + "," + item.getString("serviceName"));
            JSONObject Disk = item.getJSONObject("Disk");
            JSONObject Ram = item.getJSONObject("RAM");
            result.append(item.getDouble("CPU")).append(",").append(Disk.getDouble("Free") / Disk.getDouble("Total"))
                    .append(",").append(Ram.getDouble("Free") / Ram.getDouble("Total"))
                    .append(",").append(item.getLong("Timestamp"));
            value.set(result.toString());
            context.write(key, value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        final String DELIMITER = ",";
        private final Text value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double CPU = 0, Disk = 0, Ram = 0, temp;
            double peakCpu = 0, peakDisk = 0, peakRam = 0;
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
            context.write(key, value);
        }
    }

    private static String getDate(Long timeStamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
        return simpleDateFormat.format(new Date(timeStamp));
    }
    private static long getTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
        return simpleDateFormat.parse(Day).getTime();
    }

    public static void analyze() throws Exception{
        Configuration conf = new Configuration();
        conf.set("from", "1");
        conf.set("to", "2");
        Job job = Job.getInstance(conf, "BetweenDate");
        job.setJarByClass(BetweenDate.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("analysis"));
        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws Exception {
//        analyze();

        Date d = new Date(getTimeStamp("02-04-2022"));
        System.out.println(d);
    }
}
