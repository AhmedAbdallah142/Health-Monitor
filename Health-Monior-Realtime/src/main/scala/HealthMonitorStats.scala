import java.sql.Timestamp

case class HealthMonitorStats(service: String, time: Timestamp, count: Long,
                              ACpu: Double, PCpu: Double, TCpu: Timestamp,
                              ADisk: Double, PDisk: Double, TDisk: Timestamp,
                              ARam: Double, PRam: Double, TRam: Timestamp
                             )
