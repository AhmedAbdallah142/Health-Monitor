package Bigdata.Controller;

import Bigdata.dataBase.Duck_db;
import Bigdata.models.serviceModel;
import org.springframework.web.bind.annotation.*;

import java.sql.Timestamp;
import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("/Data")
public class getAnalytics {

    @GetMapping("/getData")
    public List<serviceModel> getTreeById(@RequestParam String from, @RequestParam String to) throws Exception {
        System.out.println(from);
        System.out.println(to);
        Timestamp From = Timestamp.valueOf(from);
        Timestamp To = Timestamp.valueOf(to);
        System.out.println(From);
        System.out.println(To);
        return Duck_db.query(From,To);
    }

}
