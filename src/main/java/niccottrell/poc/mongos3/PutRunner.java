package niccottrell.poc.mongos3;

import org.bson.Document;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * A putter than puts a file to S3 in parallel
 */
public class PutRunner implements Callable<Integer> {

    private final Demo demo;

    private final Collection<Document> docs;

    public PutRunner(Demo demo, Collection<Document> docs) {
        this.demo = demo;
        this.docs = docs;
    }

    @Override
    public Integer call() {
        int count = 0;
        try {
            for (Document doc : docs) {
                demo.saveS3(doc);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

}
