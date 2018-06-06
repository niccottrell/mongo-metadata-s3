package niccottrell.poc.mongos3;

import org.bson.Document;

import java.util.concurrent.Callable;

/**
 * A putter than puts a file to S3 in parallel
 */
public class PutRunner implements Callable<Integer> {

    private final Demo demo;
    private final Document doc;

    public PutRunner(Demo demo, Document doc) {
        this.demo = demo;
        this.doc = doc;
    }

    @Override
    public Integer call() {
        try {
            demo.saveS3(doc);
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

}
