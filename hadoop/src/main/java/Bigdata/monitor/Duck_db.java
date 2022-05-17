package Bigdata.monitor;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class Duck_db {
    public static void query(Timestamp start, Timestamp end) throws ClassNotFoundException, SQLException {
        LocalDateTime s1 = start.toLocalDateTime();
        LocalDate d1 = s1.toLocalDate();
        LocalDateTime endOfDate = d1.atTime(LocalTime.MAX);
        Timestamp night1 = Timestamp.valueOf(endOfDate);

        LocalDateTime s2 = start.toLocalDateTime();
        LocalDate d2 = s2.toLocalDate();
        Timestamp day2 = Timestamp.valueOf(LocalDateTime.of(d1, LocalTime.MIDNIGHT));

        // start -> night1  {minutes}
        // night1 -> day2  {days}
        // day2 -> end  {minutes}

        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM 'Day-r-00000.snappy.parquet' WHERE time BETWEEN '"+start+"' AND '"+end+"'");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }
}
