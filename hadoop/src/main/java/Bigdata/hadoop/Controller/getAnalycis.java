package Bigdata.hadoop.Controller;

import Bigdata.hadoop.Service;
import Bigdata.hadoop.getDataFile;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static Bigdata.hadoop.mapReduce.BetweenDate.analyze;

@RestController
@CrossOrigin
@RequestMapping("/Data")
public class getAnalycis {

    @GetMapping("/getdata")
    public List<Service> getTreeById(@RequestParam String from, @RequestParam String to) throws Exception {
        analyze(from, to);
        getDataFile res = new getDataFile();
        String finalResult = res.getAllData();
        String[] Ser = finalResult.split("\\n");
        List<Service> services = new ArrayList<>();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");

        for(int i = 0 ; i < Ser.length; i++){
            String[] words = Ser[i].split("\\t");
            Service re = new Service();
            re.Name = words[0];

            String[] data = words[1].split(",");

            re.Count = data[0];
            re.meanCPU = data[1].substring(0, Math.min(6, data[1].length()));
            re.peakCPU = simpleDateFormat.format(new Date(Long.parseLong(data[2])));
            re.meanDisk = data[3].substring(0, Math.min(6, data[3].length()));
            re.peakDisk = simpleDateFormat.format(new Date(Long.parseLong(data[4])));
            re.meanRAM = data[5].substring(0, Math.min(6, data[5].length()));
            re.peakRAM = simpleDateFormat.format(new Date(Long.parseLong(data[6])));

            services.add(re);
        }
        return services;
    }
}
