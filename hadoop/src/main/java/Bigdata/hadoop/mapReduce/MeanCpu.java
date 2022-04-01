package Bigdata.hadoop.mapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

public class MeanCpu {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text key = new Text();

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            JSONObject item = new JSONObject(text.toString());
            key.set(item.getString("serviceName"));
            output.collect(key, new DoubleWritable(item.getDouble("CPU")));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }
            output.collect(key, new DoubleWritable(sum/count));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(MeanCpu.class);
        conf.setJobName("meanCPU");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MeanCpu.Map.class);
        conf.setReducerClass(MeanCpu.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}