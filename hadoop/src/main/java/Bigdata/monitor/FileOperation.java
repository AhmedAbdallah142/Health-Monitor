package Bigdata.monitor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class FileOperation {

    public String AddLogFile(FileSystem fileSystem, String content, String dest) throws IOException {

        Path destPath = new Path(dest);
        if (!fileSystem.exists(destPath)) {
            CreateFile(fileSystem,  content,  dest);
            return "Done";
        }
        else{
            boolean isAppendable = Boolean.parseBoolean(fileSystem.getConf().get("dfs.support.append"));
//            fileSystem.setReplication(new Path(dest), (short)1);
            if(isAppendable) {
                FSDataOutputStream fs_append = fileSystem.append(destPath);
                PrintWriter writer = new PrintWriter(fs_append);
                writer.append(content);
                writer.flush();
                fs_append.hflush();
                writer.close();
                fs_append.close();
                return "Success";
            }
            else {
                System.err.println("Please set the dfs.support.append property to true");
                return "Failure";
            }
        }

    }

    public void CreateFile(FileSystem fileSystem, String content, String dest) throws IOException {
        Path hdfsWritePath = new Path(dest);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(content);
        bufferedWriter.close();
    }

    public void ReadFile(FileSystem fileSystem, String dest) throws IOException {
        Path hdfsReadPath = new Path(dest);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        String out= IOUtils.toString(inputStream, "UTF-8");
        System.out.println(out);
        inputStream.close();
    }

    public  FileSystem configureFileSystem(String coreSitePath, String hdfsSitePath) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("dfs.support.append", true);
            Path coreSite = new Path(coreSitePath);
            Path hdfsSite = new Path(hdfsSitePath);
            conf.addResource(coreSite);
            conf.addResource(hdfsSite);
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println("Error occurred while configuring FileSystem");
        }
        return fileSystem;
    }

    public  void closeFileSystem(FileSystem fileSystem){
        try {
            fileSystem.close();
        }
        catch (IOException ex){
            System.out.println("----------Could not close the FileSystem----------");
        }
    }
}