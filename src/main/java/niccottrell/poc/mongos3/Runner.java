package niccottrell.poc.mongos3;

import niccottrell.poc.mongos3.workers.Archiver;
import niccottrell.poc.mongos3.workers.Populator;
import niccottrell.poc.mongos3.workers.Putter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Runner {

    private final Demo demo;

    public Runner(Demo demo) {
        this.demo = demo;
    }

    public void runTest() {

        Results testResults = demo.getResults();

        // Report on progress by looking at testResults
        Reporter reporter = new Reporter(testResults, demo);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(reporter, 0, demo.getSettings().reportTime, TimeUnit.SECONDS);

        // Using a thread pool we keep filled
        ExecutorService testexec = Executors
                .newFixedThreadPool(demo.getSettings().numThreads + 2);

        // Build populator threads
        for (int i = 0; i < demo.getSettings().numThreads; i++) {
            testexec.execute(new Populator(demo, i));
        }

        // Build S3 putter thread
        testexec.execute(new Putter(demo));

        // Build archiver thread
        testexec.execute(new Archiver(demo));

        testexec.shutdown();

        try {
            testexec.awaitTermination(Long.MAX_VALUE,
                    TimeUnit.SECONDS);
            //System.out.println("All Threads Complete: " + b);
            executor.shutdown();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        // do final report
        reporter.finalReport();

    }

}
