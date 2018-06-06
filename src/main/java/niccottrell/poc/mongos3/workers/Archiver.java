package niccottrell.poc.mongos3.workers;

import niccottrell.poc.mongos3.Demo;
import niccottrell.poc.mongos3.Results;
import org.bson.Document;

import java.util.Date;

/**
 * Replaces full document with archived version after certain time interval
 */
public class Archiver extends Worker {

    /**
     * How many seconds to wait before converting MongoDB document to archive version
     */
    long delay = 120;

    public Archiver(Demo demo) {
        super(demo, 1000);
    }

    @Override
    public void run() {
        System.out.println("Starting document archiver");
        while (keepGoing()) {
            int count = 0;
            Date before = new Date(System.currentTimeMillis() - delay * 1000);
            Iterable<Document> docs = demo.findFull(before);
            for (Document doc : docs) {
                demo.archive(doc);
                count++;
            }
            incOps(Results.Op.ARCHIVES, count);
            sleep();
        }
    }

}
