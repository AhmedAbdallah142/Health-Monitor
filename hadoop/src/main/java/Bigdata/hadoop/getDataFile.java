package Bigdata.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import static Bigdata.hadoop.mapReduce.BetweenDate.analyze;

public class getDataFile {

    public  String getAllData() throws IOException {
        String result = ReadFileData("result/part-r-00000");
        deleteDirectoryRecursionJava6(new File("result"));
        return result;
    }

    void deleteDirectoryRecursionJava6(File file) throws IOException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectoryRecursionJava6(entry);
                }
            }
        }
        if (!file.delete()) {
            throw new IOException("Failed to delete " + file);
        }
    }

    public  String ReadFileData(String name){
        String data="";
        try {
            File myObj = new File(name);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                data += myReader.nextLine()+"\n";
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return data;
    }

//    public static void main(String[] args) throws Exception {
//        analyze("01-04-1930", "02-04-2022");
//        System.out.println(getAllData());
//    }
}
