package Bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BigDataApp {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
		System.setProperty("HADOOP_USER_NAME", "hadoopuser");
		SpringApplication.run(BigDataApp.class, args);
	}
}
