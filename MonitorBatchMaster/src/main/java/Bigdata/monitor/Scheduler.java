package Bigdata.monitor;


import Bigdata.servingLayer.ServingLayer;
import Bigdata.monitor.FileMonitor.FileOperation;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
public class Scheduler {
    ServingLayer s = new ServingLayer();

    @Scheduled(cron = "0 0 0 * * *")
    public void scheduleTaskUsingCronExpression() throws Exception {
        FileOperation.rename("Data/RealTime/Current","Data/RealTime/Old");
        s.analysisIncrement(TimeMonitor.getLastDay());
        FileOperation.DeleteDir("Data/RealTime/Old");
    }

}
