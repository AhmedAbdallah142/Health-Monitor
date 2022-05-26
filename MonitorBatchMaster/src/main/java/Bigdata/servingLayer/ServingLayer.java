package Bigdata.servingLayer;

import Bigdata.monitor.FileMonitor.FileOperation;
import Bigdata.servingLayer.mapReduce.Analysis;
import Bigdata.monitor.FileMonitor.HadoopFileOperation;
import org.apache.hadoop.util.ToolRunner;

public class ServingLayer {
    public void analysisRecompute() throws Exception {
        String[] args = {"hdfs://localhost:9000/Logs","../Data/Batch"};
        ToolRunner.run(new Analysis(),args);
    }
    public void analysisIncrement(String day) throws Exception {
        String[] args = {"hdfs://localhost:9000/Logs/"+day+".csv","../Data/Increment"};
        ToolRunner.run(new Analysis(),args);
        FileOperation.rename("../Data/Increment/Day/Day-r-00000.snappy.parquet","../Data/Batch/Day/Day_Inc_"+day+".snappy.parquet");
        FileOperation.rename("../Data/Increment/Min/Min-r-00000.snappy.parquet","../Data/Batch/Min/Min_Inc_"+day+".snappy.parquet");
        FileOperation.DeleteDir("../Data/Increment");
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        ServingLayer s = new ServingLayer();
//        s.analysisRecompute();
        s.analysisIncrement("20220527");
    }
}
