package boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InvokeTestApplication {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(InvokeTestApplication.class, args);
        CreateDirectory.getFileSystemValue();
    }
}
