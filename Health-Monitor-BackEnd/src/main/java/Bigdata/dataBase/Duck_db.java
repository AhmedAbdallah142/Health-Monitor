package Bigdata.dataBase;

import Bigdata.models.NewObject;
import Bigdata.models.serviceModel;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Duck_db {
    public static HashMap<String, NewObject> Update (ResultSet rs, HashMap<String,NewObject> map)throws ClassNotFoundException, SQLException{
        while (rs.next()) {
            String service = rs.getString("service");
            if (!map.containsKey(service)) {
                map.put(service, new NewObject());
            }

            int count = rs.getInt("C");
            NewObject temp = map.get(service);
            temp.setCount(temp.getCount() + count);

            double ACpu = rs.getDouble("ACpu");
//            ACpu = ACpu * count;
            temp.setACpu(temp.getACpu() + ACpu);

            double PCpu = rs.getDouble("PCpu");
            if (temp.getPCpu() < PCpu) {
                temp.setPCpu(PCpu);
            }

            double ADisk = rs.getDouble("ADisk");
//            ADisk = ADisk * count;
            temp.setADisk(temp.getADisk() + ADisk);

            double PDisk = rs.getDouble("PDisk");
            if (temp.getPDisk() < PDisk) {
                temp.setPDisk(PDisk);
            }


            double ARam = rs.getDouble("ARam");
//            ARam = ARam * count;
            temp.setARam(temp.getARam() + ARam);

            double PRam = rs.getDouble("PRam");
            if (temp.getPRam() < PRam) {
                temp.setPRam(PRam);
            }

            map.replace(service, temp);
        }
        return map;
    }
    public static ArrayList<serviceModel> query(Timestamp start, Timestamp end) throws ClassNotFoundException, SQLException {
        HashMap<String, NewObject> map = new HashMap<>();
        LocalDateTime s1 = start.toLocalDateTime();
        LocalDate d1 = s1.toLocalDate();
        LocalDateTime endOfDate = d1.atTime(LocalTime.MAX);
        Timestamp night1 = Timestamp.valueOf(endOfDate);

        LocalDateTime s2 = start.toLocalDateTime();
        LocalDate d2 = s2.toLocalDate();
        Timestamp day2 = Timestamp.valueOf(LocalDateTime.of(d1, LocalTime.MIDNIGHT));

        String MinPath = "../Data/Batch/Min/*.parquet";
        String DayPath = "../Data/Batch/Day/*.parquet";
        String RealPath = "../Data/RealTime/*/*.parquet";
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        // start -> night1  {minutes}
        ResultSet rs = stmt.executeQuery("SELECT service, SUM(count) as C,SUM(ACpu*count) as ACpu, MAX(PCpu) as PCpu ,SUM(ADisk*count) as ADisk, MAX(PDisk) as PDisk ,SUM(ARam*count) as ARam, MAX(PRam) as PRam " +
                "FROM '"+MinPath+"' WHERE (time BETWEEN '"+start+"' AND '"+night1+"') OR (time BETWEEN '"+day2+"' AND '"+end+"')" +
                "GROUP BY service;");
        map = Update(rs,map);
        // night1 -> day2  {days}
        rs = stmt.executeQuery("SELECT service, SUM(count) as C,SUM(ACpu*count) as ACpu, MAX(PCpu) as PCpu ,SUM(ADisk*count) as ADisk, MAX(PDisk) as PDisk ,SUM(ARam*count) as ARam, MAX(PRam) as PRam " +
                "FROM '"+DayPath+"' WHERE time BETWEEN '"+night1+"' AND '"+day2+"'" +
                "GROUP BY service;");
        map = Update(rs,map);
        // realtime
        rs = stmt.executeQuery("SELECT service, SUM(count) as C,SUM(ACpu*count) as ACpu, MAX(PCpu) as PCpu ,SUM(ADisk*count) as ADisk, MAX(PDisk) as PDisk ,SUM(ARam*count) as ARam, MAX(PRam) as PRam" +
                " FROM '"+RealPath+"' WHERE time BETWEEN '"+start+"' AND '"+end+"'" +
                "GROUP BY service;");
        map = Update(rs,map);
        // using iterators
        Iterator<Map.Entry<String, NewObject>> itr = map.entrySet().iterator();
        ArrayList<serviceModel> list = new ArrayList<serviceModel>();

        while(itr.hasNext())
        {
            serviceModel s = new serviceModel();
            Map.Entry<String, NewObject> entry = itr.next();
            s.Name = entry.getKey();
            int c = entry.getValue().getCount();
            s.Count = c+"";
            s.meanCPU = (entry.getValue().getACpu()/c)+"";
            s.meanDisk = (entry.getValue().getADisk()/c )+"";
            s.meanRAM = (entry.getValue().getARam()/c)+"";
            s.peakCPU = entry.getValue().getPCpu()+"";
            s.peakDisk = entry.getValue().getPDisk()+"";
            s.peakRAM = entry.getValue().getPRam()+"";
            list.add(s);
//            System.out.println("Service = " + entry.getKey() +
//                    "     " + entry.getValue().toString() );
        }
        return list;
    }
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        Timestamp t= new Timestamp(192992929);
        ArrayList<serviceModel> l = query(t,t);
        for(serviceModel s: l){
            System.out.println(s.toString());
        }
    }
}
