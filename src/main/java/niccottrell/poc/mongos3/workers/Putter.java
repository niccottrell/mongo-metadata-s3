package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.Results;
import org.bson.Document;

import java.util.Date;

/**
 * Puts objects into S3
 */
public class Putter extends Worker {

    Date lastRun;
    private int sleepMs = 1000;
    private int batchSize = 20;

    public Putter(Demo demo) {
        super(demo);
        lastRun = new Date(0); // start at the beginning of time
    }

    @Override
    public void run() {
        System.out.println("Starting S3 putter");
        while (keepGoing()) {
            // load some small number of documents to process
            Iterable<Document> docs = demo.findNew(lastRun).limit(batchSize);
            int count = demo.saveS3(docs);
            // if (count > 0) System.out.format("Just put %,d S3 objects\n", count);
            incOps(Results.Op.PUTS, count);
            lastRun = new Date(); // update the date so we don't put the same object multiple times
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
