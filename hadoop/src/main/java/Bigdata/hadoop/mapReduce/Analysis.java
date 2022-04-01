package Bigdata.hadoop.mapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

public class Analysis {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            JSONObject item = new JSONObject(text.toString());
            StringBuilder result = new StringBuilder();
            key.set(item.getString("serviceName"));
            JSONObject Disk = item.getJSONObject("Disk");
            JSONObject Ram = item.getJSONObject("RAM");
            result.append(item.getDouble("CPU")).append(",").append(Disk.getDouble("Free") / Disk.getDouble("Total"))
                    .append(",").append(Ram.getDouble("Free") / Ram.getDouble("Total"))
                    .append(",").append(item.getLong("Timestamp"));
            value.set(result.toString());
            output.collect(key, value);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        final String DELIMITER = ",";
        private final Text value = new Text();
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double CPU = 0, Disk = 0, Ram = 0;
            int count = 0;
            String[] data = new String[0];
            while (values.hasNext()) {
                data = values.next().toString().split(DELIMITER);
                CPU += Double.parseDouble(data[0]);
                Disk += Double.parseDouble(data[1]);
                Ram += Double.parseDouble(data[2]);
                count++;
            }
            value.set(CPU / count + "," + Disk / count + "," + Ram / count + "," + count + "," + data[3]);
            output.collect(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        JobConf conf = new JobConf(Analysis.class);

        conf.setJobName("Analysis");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Analysis.Map.class);
//        conf.setCombinerClass(Analysis.Reduce.class);
        conf.setReducerClass(Analysis.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
