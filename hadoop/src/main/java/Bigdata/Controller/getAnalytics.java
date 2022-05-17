package Bigdata.Controller;

import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static Bigdata.hadoop.mapReduce.BetweenDate.analyze;

@RestController
@CrossOrigin
@RequestMapping("/Data")
public class getAnalytics {

    @GetMapping("/getData")
    public List<Service> getTreeById(@RequestParam String from, @RequestParam String to) throws Exception {
        String finalResult = analyze(from, to);
        String[] Ser = finalResult.split("\\n");
        List<Service> services = new ArrayList<>();

        for (String s : Ser) {
            if(s.length() < 1)
                break;
            String[] words = s.split("\\t");
            Service re = new Service();
            re.Name = words[0];

            String[] data = words[1].split(",");
            re.Count = data[0];
            re.meanCPU = data[1].substring(0, Math.min(6, data[1].length()));
            re.peakCPU = getDate(Long.parseLong(data[2]));
            re.meanDisk = data[3].substring(0, Math.min(6, data[3].length()));
            re.peakDisk = getDate(Long.parseLong(data[4]));
            re.meanRAM = data[5].substring(0, Math.min(6, data[5].length()));
            re.peakRAM = getDate(Long.parseLong(data[6]));
            services.add(re);
        }
        return services;
    }
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
    private static String getDate(Long timeStamp) {
        timeStamp*=1000;
        return simpleDateFormat.format(new Date(timeStamp));
    }
}
