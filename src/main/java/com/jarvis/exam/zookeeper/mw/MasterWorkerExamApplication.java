package com.jarvis.exam.zookeeper.mw;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class MasterWorkerExamApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(MasterWorkerExamApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        var connection = "localhost:2181";
        var master = new Master(connection);
        master.startZk();
        log.info("run for get master");
        master.runForMaster();

        if (master.isLeader()) {
            log.info("I'm the Leader");
            /* Master Logic */
            Thread.sleep(60000);
        } else {
            log.info("Someone else is the leader");
        }

        master.stopZk();
    }
}
