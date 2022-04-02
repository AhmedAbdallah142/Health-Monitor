package Bigdata.hadoop.Controller;

import Bigdata.hadoop.Service;
import Bigdata.hadoop.getDataFile;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;

import static Bigdata.hadoop.mapReduce.BetweenDate.analyze;

@Controller
@ResponseBody
@RequestMapping("/Data")
public class getAnalycis {

    @GetMapping("/getdata")
    public List<Service> getTreeById(@RequestParam String from, @RequestParam String to) throws Exception {
        analyze(from, to);
        getDataFile res =new getDataFile();
        String finalresult =res.getAllData();
        String[] Ser=finalresult.split("\\n");//splits the string based on whitespace
        List<Service> services =new ArrayList<>();
        for(int i=0 ; i<Ser.length;i++){
            String[] words=Ser[i].split("\\t");//splits the string based on whitespace
            Service re =new Service();
            re.Name = words[0];
            re.Data = words[1];
            services.add(re);
        }
        return services;
    }
}
