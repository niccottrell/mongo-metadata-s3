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
                    .sort(new Document("_id", 1)) // assuming ObjectId take the in insert order
                    .limit(batchSize);
            // spool threads
            Collection<Future<Integer>> futures = new ArrayList<>();
            Collection<Document> buffer = new ArrayList<>();
            int sent = 0;
            for (Document doc : docs) {
                // put in 10 documents per thread
                buffer.add(doc);
                if (buffer.size() >= 10) {
                    futures.add(demo.s3Pool.submit(new PutRunner(demo, buffer)));
                    buffer = new ArrayList<>();
                }
                lastRun = doc.getObjectId("_id").getDate();
                sent++;
            }
            // anything remaining
            if (buffer.size() > 0)
                futures.add(demo.s3Pool.submit(new PutRunner(demo, buffer)));
            // System.out.format("Submitted %d put runners up to %s\n", sent, lastRun);
            // collect results
            int total = 0;
            for (Future<Integer> future : futures) {
                try {
                    Integer count = future.get();
                    incOps(Results.Op.PUTS, count);
                    total += count;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // if (count > 0) System.out.format("Just put %,d S3 objects\n", count);
            if (total == 0) {
                // there was nothing to do
                System.out.println("Ran out of documents not in S3, sleeping");
                sleep(1000);
            } else {
                sleep();
            }
        }
    }

}