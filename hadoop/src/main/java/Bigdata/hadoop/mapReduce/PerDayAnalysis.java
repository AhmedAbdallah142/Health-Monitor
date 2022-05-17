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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PerDayAnalysis {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();
        final String DELIMITER = ",";
        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            try {
                String[] data = text.toString().split(DELIMITER);
                StringBuilder result = new StringBuilder();
//                key.set(getDate(Long.parseLong(data[2]))+data[1]);
                result.append(data[0]).append(",").append(Double.parseDouble(data[3].substring(8)) / Double.parseDouble(data[4].substring(5,data[4].length()-2)))
                        .append(",").append(Double.parseDouble(data[5].substring(8)) / Double.parseDouble(data[6].substring(5,data[6].length()-2)))
                        .append(",").append(data[2]);

                System.out.println(data[5].substring(8));
//                value.set(result.toString());
//                context.write(key, value);
            }catch (Exception e){
//                System.out.println(text);
//                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Per Day Analysis");
        job.setJarByClass(PerDayAnalysis.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Logs"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/Analysis"));
        MultipleOutputs.addNamedOutput(job, "Day", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Min", TextOutputFormat.class, Text.class, Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Check"));
        FileOutputFormat.setOutputPath(job, new Path("analysis"));
        job.waitForCompletion(true);
    }
}