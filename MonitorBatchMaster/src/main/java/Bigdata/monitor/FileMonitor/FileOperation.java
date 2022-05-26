package Bigdata.monitor.FileMonitor;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.cleanDirectory;

public class FileOperation {
    public static boolean rename(String oldPath,String newPath){
        File sourceFile = new File(oldPath);
        File destFile = new File(newPath);
        return sourceFile.renameTo(destFile);
    }

    public static boolean DeleteDir(String path) throws IOException {
        File directory = new File(path);
        cleanDirectory(directory);
        return directory.delete();
    }
}
