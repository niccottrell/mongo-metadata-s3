package niccottrell.poc.mongos3;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import static niccottrell.poc.mongos3.Demo.PREFIX_FIELD;

public class DemoTest {

    @Test
    public void testPrefix1() throws ParseException {
        String[] args = {"-s3bucket", "BUCKET",
                "-s3region", "eu-central-1",
                "-s3prefix", "AUF/",
                "-skipGzip",
                "-skipPrefixHash"};
        Settings settings = new Settings(args);
        Document doc = new Document();
        doc.put("_id", "abc");
        doc.put(PREFIX_FIELD, 12345);
        Demo demo = new Demo(settings);
        String s3Key = demo.getS3Key(doc);
        Assert.assertEquals("AUF/12345/abc.json", s3Key);
    }

    @Test
    public void testPrefix2() throws ParseException {
        String[] args = {"-s3bucket", "BUCKET",
                "-s3region", "eu-central-1"};
        Settings settings = new Settings(args);
        Document doc = new Document();
        doc.put("_id", "abc");
        doc.put(PREFIX_FIELD, 4567);
        Demo demo = new Demo(settings);
        String s3Key = demo.getS3Key(doc);
        // Note: 11d7 is the hex of 4567
        // Note 2: the .gz suffix is added by prepareS3Request
        Assert.assertEquals("11d7/abc.json", s3Key);
    }

    @Test
    public void testPrepareS3Request1() throws Exception {
        String[] args = {"-s3bucket", "BUCKET",
                "-s3region", "eu-central-1"};
        Settings settings = new Settings(args);
        Document doc = new Document();
        doc.put("_id", "abc");
        doc.put(PREFIX_FIELD, 4567);
        Demo demo = new Demo(settings);
        PutObjectRequest s3Request = demo.prepareS3Request(doc);
        Assert.assertEquals("11d7/abc.json.gz", s3Request.getKey());
    }

}
