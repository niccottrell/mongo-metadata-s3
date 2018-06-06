package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.PutRunner;
import niccottrell.poc.mongos3.Results;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.Future;

/**
 * Puts objects into S3
 */
public class Putter extends Worker {

    Date lastRun;

    private int batchSize = 200;

    public Putter(Demo demo) {
        super(demo, 0);
        lastRun = new Date(); // start at the beginning of tests
    }

    @Override
    public void run() {
        // System.out.format("Starting S3 putter from %s\n", lastRun);
        while (keepGoing()) {
            // load some small number of documents to process
            Iterable<Document> docs = demo.findNew(lastRun)
                    .sort(new Document("_id", 1)) // assuming ObjectId take then in insert order
                    .limit(batchSize);
            // spool threads
            Collection<Future<Integer>> futures = new ArrayList<>();
            for (Document doc : docs) {
                futures.add(demo.s3Pool.submit(new PutRunner(demo, doc)));
                lastRun = doc.getObjectId("_id").getDate();
            }
            // System.out.format("Submitted %d put runners up to %s\n", futures.size(), lastRun);
            // collect results
            int count = 0;
            for (Future<Integer> future : futures) {
                try {
                    count += future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // if (count > 0) System.out.format("Just put %,d S3 objects\n", count);
            incOps(Results.Op.PUTS, count);
            if (count == 0) {
                // there was nothing to do
                sleep(1000);
            } else {
                sleep();
            }
        }
    }

}