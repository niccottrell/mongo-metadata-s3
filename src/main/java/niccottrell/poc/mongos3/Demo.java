package niccottrell.poc.mongos3;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class Demo {

    /**
     * The field used to build the "folder" or S3 object prefix
     */
    public static final String PREFIX_FIELD = "userId";

    /**
     * The field which stores the status of the document (full or archived)
     */
    private static final String ARCHIVED = "archived";

    /**
     * These fields are stored in MongoDB (while the other fields are considered the payload)
     */
    public static String[] metaFields = new String[]{"name", "type", "count"};

    private static final MongoClient mongoClient = MongoClients.create();

    private final Settings settings;
    private final Results results;

    public Demo(Settings settings) {
        this.settings = settings;
        this.results = new Results();
    }

    public static void main(String[] args) throws Exception {
        Settings settings = new Settings(args);
        Demo demo = new Demo(settings);
        // cleanup S3 and MongoDB
        if (settings.drop) demo.dropData();
        // write some Documents in S3 and MongoDB
        // demo.populateMongo(false);
        // prepare results oobject
        Runner runner = new Runner(demo);
        runner.runTest();
        // if requested, wipe MongoDB collection and rebuild from S3
        if (settings.testRebuild) {
            // wipe the MongoDB versions
            demo.getCollection().drop();
            // rebuild the metadata from S3
            demo.rebuild();
            // load metadata from Mongo
            Document exMeta = demo.query();
            // load full doc from S3
            Document exFull = demo.loadS3(exMeta);
            System.out.println("Metadata=" + exMeta);
            System.out.println("Full=" + exFull);
        }
    }

    private Document loadS3(Document doc) {
        String key = getS3Key(doc);
        AmazonS3 s3Client = getS3Client();
        S3Object object = s3Client.getObject(new GetObjectRequest(settings.s3Bucket, key));
        String json = readJson(object);
        return Document.parse(json);
    }

    /**
     * Restore the metadata from S3
     */
    private void rebuild() {
        AmazonS3 s3Client = getS3Client();

        String prefix = ""; // include everything
        ObjectListing listing = s3Client.listObjects(settings.s3Bucket, prefix);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }

        // TODO: process per batch rather than loading all into memory first
        for (S3ObjectSummary summary : summaries) {
            String fileName = summary.getKey();
            S3Object object = s3Client.getObject(new GetObjectRequest(settings.s3Bucket, fileName));
            // load the JSON from S3
            String json = readJson(object);
            // restore the document from JSON
            Document doc = Document.parse(json);
            // Save the metadata like during initial save
            saveMongo(doc);
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
        database.getCollection(settings.collectionName).drop();
        deleteS3Objects("");
    }

    /**
     * @param prefix
     */
    private void deleteS3Objects(String prefix) {

        AmazonS3 s3Client = getS3Client();

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(settings.s3Bucket)
                .withPrefix(prefix);

        ObjectListing objectsResponse = s3Client.listObjects(listObjectsRequest);

        while (true) {

            ArrayList<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();

            for (Iterator<S3ObjectSummary> iterator = objectsResponse.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                S3ObjectSummary s3Object = iterator.next();
                keysToDelete.add(new DeleteObjectsRequest.KeyVersion(s3Object.getKey()));
            }

            if (!keysToDelete.isEmpty()) {
                DeleteObjectsRequest delReq = new DeleteObjectsRequest(settings.s3Bucket);
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

    private MongoDatabase getDb() {
        return mongoClient.getDatabase(settings.databaseName);
    }

    public MongoCollection<Document> getCollection() {
        MongoDatabase database = getDb();
        return database.getCollection(settings.collectionName);
    }

    private Document query() {
        MongoCollection<Document> collection = getCollection();
        FindIterable<Document> documents = collection.find(
                new Document("count", new Document("$gte", (int) (Math.random() * 100f))));
        return documents.first();
    }

    public void populateMongo(int batchSize, boolean metaOnly) {
        MongoCollection<Document> collection = getCollection();
        List<Document> batch = new ArrayList<Document>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Document doc = createDoc();
            batch.add(metaOnly ? createMeta(doc) : doc);
        }
        // insert in batch for performance
        collection.insertMany(batch);
    }

    /**
     * Archive a document
     */
    private void archive(ObjectId id) {
        MongoCollection<Document> collection = getCollection();
        Bson filter = Filters.eq(id);
        Document full = collection.find(filter).first();
        Document metadata = createMeta(full);
        collection.replaceOne(filter, metadata);
    }

    /**
     * Archive a document
     */
    public void archive(Document full) {
        MongoCollection<Document> collection = getCollection();
        Bson filter = Filters.eq(full.getObjectId("_id"));
        Document metadata = createMeta(full);
        collection.replaceOne(filter, metadata);
    }

    /**
     * Save the metadata to Mongo
     *
     * @param doc The full document (including metadata and payload fields)
     */
    private void saveMongo(Document doc) {
        Document metadata = createMeta(doc);
        MongoCollection<Document> collection = getCollection();
        collection.insertOne(metadata);
    }

    /**
     * @return a subset of the original document with just meta fields (but same _id)
     */
    private Document createMeta(Document doc) {
        Document metadata = new Document();
        metadata.put("_id", doc.get("_id"));
        for (String metaField : metaFields) {
            metadata.put(metaField, doc.get(metaField));
        }
        // make sure the prefix is stored too (even if not explicity in meta fields)
        metadata.put(PREFIX_FIELD, doc.get(PREFIX_FIELD));
        metadata.put(ARCHIVED, true);
        return metadata;
    }

    /**
     * @return a full dummy doc
     */
    private Document createDoc() {
        int userId = (int) (Math.random() * 1000f);
        Document doc = new Document("_id", new ObjectId())
                .append(PREFIX_FIELD, userId)
                .append("name", "MongoDB")
                .append("type", "database")
                .append("count", (int) (Math.random() * 1000f))
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("data", RandomStringUtils.randomAlphanumeric(64))
                .append("info", new Document("x", 203).append("y", 102))
                .append(ARCHIVED, false); // if false, full document (otherwise only metadata) in MongoDB)
        if (settings.fieldCount > 8) {
            for (int i = 7; i < settings.fieldCount; i++) {
                doc.append("fld" + i, RandomStringUtils.randomAlphanumeric(16));
            }
        }
        return doc;
    }

    /**
     * Put a single document to S3
     */
    public void saveS3(Document doc) {

        AmazonS3 s3Client = getS3Client();
        try {
            PutObjectRequest request = prepareS3Request(doc);
            s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
    }

    /**
     * Put a multiple document to S3 in parallel
     */
    public int saveS3(Iterable<Document> docs) {

        int count = 0;

        AmazonS3 s3Client = getS3Client();
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        for (Document doc : docs) {
            try {
                PutObjectRequest request = prepareS3Request(doc);
                tm.upload(request);
                count++;
            } catch (AmazonServiceException e) {
                // The call was transmitted successfully, but Amazon S3 couldn't process
                // it, so it returned an error response.
                e.printStackTrace();
            }
        }
        return count;

    }

    private PutObjectRequest prepareS3Request(Document doc) {
        String fileObjKeyName = getS3Key(doc);

        // Convert Document to JSON string to input stream
        String json = doc.toJson();
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        InputStream stream = new ByteArrayInputStream(bytes);

        // Upload a file as a new object with ContentType and title specified.

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/json");  // TODO: save as tar.gz
        metadata.setContentLength(bytes.length);
        metadata.addUserMetadata("x-amz-meta-title", fileObjKeyName);

        return new PutObjectRequest(settings.s3Bucket, fileObjKeyName, stream, metadata);
    }

    private String getS3Key(Document doc) {
        return settings.s3Prefix + doc.get(PREFIX_FIELD) + "/" +
                doc.getObjectId("_id").toHexString() + ".json";
    }

    private AmazonS3 s3client;

    private AmazonS3 getS3Client() {
        if (s3client == null) {
            s3client = AmazonS3ClientBuilder.standard()
                    .withRegion(settings.s3Region)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
        }
        return s3client;
    }

    public Settings getSettings() {
        return settings;
    }

    public FindIterable<Document> findNew(Date lastRun) {
        return getCollection().find(Filters.gt("_id", new ObjectId(lastRun)));
    }

    public FindIterable<Document> findFull(Date before) {
        return getCollection().find(Filters.and(
                Filters.lte("_id", new ObjectId(before)),
                Filters.eq(ARCHIVED, false)));
    }

    public Results getResults() {
        return results;
    }
}
