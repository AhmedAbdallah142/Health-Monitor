package Bigdata.hadoop;

import Bigdata.hadoop.mapReduce.Analysis;
import Bigdata.monitor.FileOperation;
import org.apache.hadoop.util.ToolRunner;

public class ServingLayer {
    public void analysisRecompute() throws Exception {
        String[] args = {"Logs","Analysis"};
        ToolRunner.run(new Analysis(),args);
    }
    public void analysisIncrement(String day) throws Exception {
        FileOperation f = new FileOperation();
        String[] args = {"Logs/"+day+".csv","Increment"};
        ToolRunner.run(new Analysis(),args);
        f.MoveFile("hdfs://localhost:9000/Increment/Day/Day-r-00000.snappy.parquet","hdfs://localhost:9000/Analysis/Day/Day_Inc_"+day+".snappy.parquet");
        f.MoveFile("hdfs://localhost:9000/Increment/Min/Min-r-00000.snappy.parquet","hdfs://localhost:9000/Analysis/Min/Min_Inc_"+day+".snappy.parquet");
        f.DeleteFile("hdfs://localhost:9000/Increment");
        f.closeFileSystem();
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        ServingLayer s = new ServingLayer();
        s.analysisIncrement("20220524");
    }
}
