package niccottrell.poc.mongos3;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.mongodb.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

public class Demo {

    public static final String DBNAME = "s3poc";
    public static final String COLLNAME = "meta";
    public static final String COLL_RESTORED = COLLNAME + ".restored";

    /**
     * The field used to build the "folder" or S3 object prefix
     */
    public static final String PREFIX_FIELD = "userId";
    public static String s3clientRegion = "eu-central-1";
    public static String s3bucketName = "nic-metadata-poc";

    /**
     * These fields are stored in MongoDB (while the other fields are considered the payload)
     */
    public static String[] metaFields = new String[]{"name", "type", "count"};

    private static final MongoClient mongoClient = MongoClients.create();

    public static void main(String[] args) {
        Demo demo = new Demo();
        // cleanup S3 and MongoDB
        demo.dropData();
        // write some Documents in S3 and MongoDB
        demo.populate();
        // wipe the MongoDB versions
        MongoDatabase database = getDb();
        database.getCollection(COLLNAME).drop();
        // rebuild the metadata from S3
        demo.rebuild(COLLNAME);
        // load metadata from Mongo
        Document exMeta = demo.query();
        // load full doc from S3
        Document exFull = demo.loadS3(exMeta);
        System.out.println("Metadata=" + exMeta);
        System.out.println("Full=" + exFull);
    }

    private Document loadS3(Document doc) {
        String key = getS3Key(doc);
        AmazonS3 s3Client = getS3Client();
        S3Object object = s3Client.getObject(new GetObjectRequest(s3bucketName, key));
        String json = readJson(object);
        return Document.parse(json);
    }

    /**
     * Restore the metadata from S3
     *
     * @param collName The collection to rebuild the metadata
     */
    private void rebuild(String collName) {
        AmazonS3 s3Client = getS3Client();

        String prefix = ""; // include everything
        ObjectListing listing = s3Client.listObjects(s3bucketName, prefix);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }

        // TODO: process per batch rather than loading all into memory first
        for (S3ObjectSummary summary : summaries) {
            String fileName = summary.getKey();
            S3Object object = s3Client.getObject(new GetObjectRequest(s3bucketName, fileName));
            // load the JSON from S3
            String json = readJson(object);
            // restore the document from JSON
            Document doc = Document.parse(json);
            // Save the metadata like during initial save
            saveMongo(doc, collName);
        }

    }

    private static String readJson(S3Object object) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            StringBuilder buf = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                buf.append(line).append('\n');
            }
            return buf.toString();
        } catch (IOException e) {
            throw new RuntimeException("Error loading S3 content: " + object.getKey(), e);
        }
    }

    /**
     * Drop both collections and S3 files
     */
    private void dropData() {
        MongoDatabase database = getDb();
        database.getCollection(COLLNAME).drop();
        database.getCollection(COLL_RESTORED).drop();
        deleteS3Objects("");
    }

    /**
     * @param prefix
     */
    private void deleteS3Objects(String prefix) {

        AmazonS3 s3Client = getS3Client();

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(s3bucketName)
                .withPrefix(prefix);

        ObjectListing objectsResponse = s3Client.listObjects(listObjectsRequest);

        while (true) {

            ArrayList<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();

            for (Iterator<S3ObjectSummary> iterator = objectsResponse.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                S3ObjectSummary s3Object = iterator.next();
                keysToDelete.add(new DeleteObjectsRequest.KeyVersion(s3Object.getKey()));
            }

            if (!keysToDelete.isEmpty()) {
                DeleteObjectsRequest delReq = new DeleteObjectsRequest(s3bucketName);
                delReq.setKeys(keysToDelete);
                s3Client.deleteObjects(delReq);
                System.out.println("Deleted s3 objects: " + keysToDelete.size());
            }

            if (objectsResponse.isTruncated()) {
                objectsResponse = s3Client.listObjects(listObjectsRequest);
                continue;
            }

            break;
        }

    }

    private static MongoDatabase getDb() {
        return mongoClient.getDatabase(DBNAME);
    }

    private static MongoCollection<Document> getCollection() {
        MongoDatabase database = getDb();
        return database.getCollection(COLLNAME);
    }

    private Document query() {
        MongoCollection<Document> collection = getCollection();
        FindIterable<Document> documents = collection.find(
                new Document("count", new Document("$gte", (int) (Math.random() * 100f))));
        return documents.first();
    }

    private void populate() {
        Document doc = createDoc();
        saveS3(doc);
        saveMongo(doc, COLLNAME);
    }

    /**
     * Save the metadata to Mongo
     *
     * @param doc      The full document (including metadata and payload fields)
     * @param collName The collection to save into
     */
    private static void saveMongo(Document doc, String collName) {
        Document metadata = new Document();
        metadata.put("_id", doc.get("_id"));
        for (String metaField : metaFields) {
            metadata.put(metaField, doc.get(metaField)); // TODO: handle deep field names like "sub1.field2"
        }
        metadata.put(PREFIX_FIELD, doc.get(PREFIX_FIELD)); // make sure the prefix is stored too (even if not explicity in meta fields)
        MongoDatabase database = getDb();
        MongoCollection<Document> collection = database.getCollection(collName);
        collection.insertOne(metadata);
    }

    /**
     * @return a full dummy doc
     */
    private static Document createDoc() {
        int userId = (int) (Math.random() * 1000f);
        return new Document("_id", new ObjectId())
                .append(PREFIX_FIELD, userId)
                .append("name", "MongoDB")
                .append("type", "database")
                .append("count", (int) (Math.random() * 1000f))
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("data", RandomStringUtils.randomAlphanumeric(64))
                .append("info", new Document("x", 203).append("y", 102));
    }

    public void saveS3(Document doc) {

        String fileObjKeyName = getS3Key(doc);

        try {
            AmazonS3 s3Client = getS3Client();

            // Convert Document to JSON string to input stream
            String json = doc.toJson();
            InputStream stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

            // Upload a file as a new object with ContentType and title specified.

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/json");  // TODO: save as tar.gz
            metadata.addUserMetadata("x-amz-meta-title", fileObjKeyName);

            PutObjectRequest request = new PutObjectRequest(s3bucketName, fileObjKeyName, stream, metadata);
            s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
    }

    private static String getS3Key(Document doc) {
        return "" + doc.get(PREFIX_FIELD) + "/" +
                doc.getObjectId("_id").toHexString() + ".json";
    }

    private static AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(s3clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

}
