package Bigdata.hadoop.mapReduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.text.ParseException;

import static Bigdata.monitor.TimeMonitor.*;

public class Analysis extends Configured implements Tool {
    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
            """
                    {
                    	"type":	"record",
                    	"name":	"testFile",
                    	"doc":	"test records",
                    	"fields":
                    	[
                    			{"name":	"service", "type":	"string"},
                    			{"name":	"time", "type": {"type": "long","logicalType": "timestamp-millis"}},
                    			{"name":    "count",	"type":	"long"},
                    			{"name":	"ACpu", "type":	"double"},
                    			{"name":	"PCpu", "type":	"double"},
                    			{"name":	"TCpu", "type": {"type": "long","logicalType": "timestamp-millis"}},
                    			{"name":	"ADisk", "type":	"double"},
                    			{"name":	"PDisk", "type":	"double"},
                    			{"name":	"TDisk", "type": {"type": "long","logicalType": "timestamp-millis"}},
                    			{"name":	"ARam", "type":	"double"},
                    			{"name":	"PRam", "type":	"double"},
                    			{"name":	"TRam", "type": {"type": "long","logicalType": "timestamp-millis"}}
                    	]
                    }
                    """);
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();
        final String DELIMITER = ",";

        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
                String[] data = text.toString().split(DELIMITER);
                value.set(data[0] + "," + Double.parseDouble(data[3].substring(8)) / Double.parseDouble(data[4].substring(5, data[4].length() - 2)) +
                        "," + Double.parseDouble(data[5].substring(8)) / Double.parseDouble(data[6].substring(5, data[6].length() - 2)) +
                        "," + data[2]);
                key.set(getDay(Long.parseLong(data[2]))+data[1]);
                context.write(key, value);
                key.set(getMin(Long.parseLong(data[2]))+data[1]);
                context.write(key, value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        final String DELIMITER = ",";
        private MultipleOutputs<Text, Text> mos;
        private final GenericRecord record = new GenericData.Record(AVRO_SCHEMA);

        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            setValue(values);
            try {
                if (k.charAt(0) == 58) {
                    record.put("service", k.substring(9));
                    record.put("time", getDayTimeStamp(k.substring(0, 9)));
                    mos.write("Day", null, record, "Day/Day");
                } else {
                    record.put("service", k.substring(13));
                    record.put("time", getMinTimeStamp(k.substring(0, 13)));
                    mos.write("Min", null, record, "Min/Min");
                }

            } catch (ParseException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        private void setValue(Iterable<Text> values) {
            double CPU = 0, Disk = 0, Ram = 0, temp, peakCpu = 0, peakDisk = 0, peakRam = 0;
            long tPeakCpu = 0, tPeakDisk = 0, tPeakRam = 0;
            long count = 0;
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
            record.put("count", count);
            record.put("ACpu", CPU / count);
            record.put("PCpu", peakCpu);
            record.put("TCpu", tPeakCpu);
            record.put("ADisk", Disk / count);
            record.put("PDisk", peakDisk);
            record.put("TDisk", tPeakDisk);
            record.put("ARam", Ram / count);
            record.put("PRam", peakRam);
            record.put("TRam", tPeakRam);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "Analysis Parquet Batch");
        job.setJarByClass(Analysis.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/"+strings[0]));
        ParquetOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/"+strings[1]));

        MultipleOutputs.addNamedOutput(job, "Day", AvroParquetOutputFormat.class, Void.class, Void.class);
        MultipleOutputs.addNamedOutput(job, "Min", AvroParquetOutputFormat.class, Void.class, Void.class);
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setEnableDictionary(job, true);
        AvroParquetOutputFormat.setSchema(job, AVRO_SCHEMA);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        int exitFlag = ToolRunner.run(new Analysis(), args);
        System.exit(exitFlag);
    }
}