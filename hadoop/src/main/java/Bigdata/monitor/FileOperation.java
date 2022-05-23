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

    private static FileOperation file = null;
    private FileSystem fileSystem = null;

    public FileOperation() {
        configureFileSystem();
    }

//    public static FileOperation getInstance() {
//        if (file == null) {
//            file = new FileOperation();
//        }
//        return file;
//    }


    final String coreSite = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    final String hdfsSite = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml";

    public String AddLogFile(String content, String dest) throws IOException {

        Path destPath = new Path(dest);
        if (!fileSystem.exists(destPath)) {
            CreateFile(content, dest);
            return "Done";
        } else {
            boolean isAppendable = Boolean.parseBoolean(fileSystem.getConf().get("dfs.support.append"));
//            fileSystem.setReplication(new Path(dest), (short)1);
            if (isAppendable) {
                FSDataOutputStream fs_append = fileSystem.append(destPath);
                PrintWriter writer = new PrintWriter(fs_append);
                writer.append(content);
                writer.flush();
                fs_append.hflush();
                writer.close();
                fs_append.close();
                return "Success";
            } else {
                System.err.println("Please set the dfs.support.append property to true");
                return "Failure";
            }
        }

    }

    public void CreateFile(String content, String dest) throws IOException {
        Path hdfsWritePath = new Path(dest);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(content);
        bufferedWriter.close();
    }

    public boolean DeleteFile(String dest) throws IOException {
        Path hdfsPath = new Path(dest);
        return fileSystem.delete(hdfsPath, true);
    }

    public boolean MoveFile(String oldPath , String newPath) throws IOException {
        return fileSystem.rename(new Path(oldPath),new Path(newPath));
    }

    public String ReadFile(String dest) throws IOException {
        Path hdfsReadPath = new Path(dest);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        String out = IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        return out;
    }

    private void configureFileSystem() {
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("dfs.support.append", true);
            Path coreSite = new Path(this.coreSite);
            Path hdfsSite = new Path(this.hdfsSite);
            conf.addResource(coreSite);
            conf.addResource(hdfsSite);
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println("Error occurred while configuring FileSystem");
        }
    }

    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException ex) {
            System.out.println("----------Could not close the FileSystem----------");
        }
    }
}