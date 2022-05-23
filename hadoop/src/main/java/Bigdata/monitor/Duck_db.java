package Bigdata.monitor;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Duck_db {
    public static HashMap<String,Object> Update (ResultSet rs, HashMap<String,Object> map)throws ClassNotFoundException, SQLException{
        while (rs.next()) {
            String service = rs.getString("service");
            if (!map.containsKey(service)) {
                map.put(service, new Object());
            }

            int count = rs.getInt("count");
            Object temp = map.get(service);
            temp.setCount(temp.getCount() + count);

            double ACpu = rs.getDouble("ACpu");
            ACpu = ACpu * count;
            temp.setACpu(temp.getACpu() + ACpu);

            double PCpu = rs.getDouble("PCpu");
            String TCpu = rs.getString("TCpu");
            if (temp.getPCpu() < PCpu) {
                temp.setPCpu(PCpu);
                temp.setTCpu(TCpu);
            }

            double ADisk = rs.getDouble("ADisk");
            ADisk = ADisk * count;
            temp.setADisk(temp.getADisk() + ADisk);

            double PDisk = rs.getDouble("PDisk");
            String TDisk = rs.getString("TDisk");
            if (temp.getPDisk() < PDisk) {
                temp.setPDisk(PDisk);
                temp.setTDisk(TDisk);
            }


            double ARam = rs.getDouble("ARam");
            ARam = ARam * count;
            temp.setARam(temp.getARam() + ARam);

            double PRam = rs.getDouble("PRam");
            String TRam = rs.getString("TRam");
            if (temp.getPRam() < PRam) {
                temp.setPRam(PRam);
                temp.setTRam(TRam);
            }

            map.replace(service, temp);
        }
        return map;
    }
    public static void query(Timestamp start, Timestamp end) throws ClassNotFoundException, SQLException {
        HashMap<String, Object> map = new HashMap<>();
        LocalDateTime s1 = start.toLocalDateTime();
        LocalDate d1 = s1.toLocalDate();
        LocalDateTime endOfDate = d1.atTime(LocalTime.MAX);
        Timestamp night1 = Timestamp.valueOf(endOfDate);

        LocalDateTime s2 = start.toLocalDateTime();
        LocalDate d2 = s2.toLocalDate();
        Timestamp day2 = Timestamp.valueOf(LocalDateTime.of(d1, LocalTime.MIDNIGHT));

        System.setProperty("fs.default.name","hdfs://localhost:9000");
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        //ResultSet rs = stmt.executeQuery("SELECT * FROM 'https://github.com/Teradata/kylo/blob/master/samples/sample-data/parquet/userdata1.parquet'");
        ResultSet rs = stmt.executeQuery("SELECT * FROM 'Day-r-00000.snappy.parquet' WHERE time BETWEEN '2022-03-21 22:00:00.0' AND '2022-03-21 22:00:00.0';");
        map = Update(rs,map);
        // night1 -> day2  {days}
        rs = stmt.executeQuery("SELECT * FROM 'Day-r-00000.snappy.parquet' WHERE time BETWEEN '2022-03-21 22:00:00.0' AND '2022-03-21 22:00:00.0';");
        map = Update(rs,map);
        // day2 -> end  {minutes}
        rs = stmt.executeQuery("SELECT * FROM 'Day-r-00000.snappy.parquet' WHERE time BETWEEN '2022-03-21 22:00:00.0' AND '2022-03-21 22:00:00.0';");
        map = Update(rs,map);
        // realtime
        rs = stmt.executeQuery("SELECT * FROM 'Day-r-00000.snappy.parquet' WHERE time BETWEEN '2022-03-21 22:00:00.0' AND '2022-03-21 22:00:00.0';");
        map = Update(rs,map);
        // using iterators
        Iterator<Map.Entry<String, Object>> itr = map.entrySet().iterator();

        while(itr.hasNext())
        {
            Map.Entry<String, Object> entry = itr.next();
            System.out.println("Service = " + entry.getKey() +
                    "     " + entry.getValue().toString() );
        }
    }
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoopuser");
        Timestamp t= new Timestamp(192992929);
        query(t,t);
    }
}
