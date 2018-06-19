package niccottrell.poc.mongos3;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class Settings {

    protected final String s3Bucket;
    protected final String s3Region;
    protected final String s3Profile;
    protected final String s3Prefix;
    protected final int s3Threads;
    public String awsRoleArn;
    protected final String mongoUri;
    protected final String databaseName;
    protected final String collectionName;
    protected final boolean drop;
    protected final boolean helpOnly;
    public int fieldCount;
    public boolean testRebuild;
    public int numThreads = 4;
    public long reportTime = 10; // seconds between reports
    public long duration = 300; // seconds to run the test
    public boolean skipGzip; // if true, save to S3 as raw JSON
    public boolean skipPrefixHash; // if true, do simple toString() on prefix field value

    public Settings(String[] args) throws ParseException {

        CommandLineParser parser = new DefaultParser();

        Options cliopt = new Options();
        cliopt.addOption("h", "help", false, "Print help only");
        cliopt.addOption("b", "s3bucket", true, "S3 bucket name");
        cliopt.addOption("r", "s3region", true, "S3 region code");
        cliopt.addOption("p", "s3profile", true, "S3 profile to use");
        cliopt.addOption(null, "s3prefix", true, "S3 prefix path (optional, e.g. `first_test/` or ``)");
        cliopt.addOption(null, "s3threads", true, "The max number of parallel S3 putObject threads, default 100");
        cliopt.addOption(null, "awsRoleArn", true, "AWS role to assume (optional, e.g. `arn:aws:iam::1234567890:role/TeamABCMember`)");
        cliopt.addOption("c", "uri", true, "MongoDB connection details (default 'mongodb://localhost:27017' )");
        cliopt.addOption("n", "namespace", true, "MongoDB namespace (default: `test.s3mongo`)");
        cliopt.addOption("d", "cleanup", false, "Drop collection and S3 data at start of run");
        cliopt.addOption("f", "fieldCount", true, "Number of fields (default: 7)");
        cliopt.addOption("t", false, "Test rebuilding of data from S3");
        cliopt.addOption(null, "skipGzip", false, "Store in S3 as JSON (not .json.gz)");
        cliopt.addOption(null, "skipPrefixHash", false, "Create S3 path based on raw field value");

        CommandLine cmd = parser.parse(cliopt, args);

        // automatically generate the help statement
        if (helpOnly = cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("mongoS3", cliopt);
        }

        s3Bucket = cmd.getOptionValue("b");
        if (isBlank(s3Bucket)) throw new RuntimeException("No S3 bucket provided");

        s3Region = cmd.getOptionValue("r");
        if (isBlank(s3Region)) throw new RuntimeException("No S3 region provided");

        s3Profile = cmd.getOptionValue("s3profile", null);
        if (s3Profile != null) System.out.println("S3 profile: " + s3Profile);

        s3Prefix = StringUtils.trim(cmd.getOptionValue("s3prefix", ""));
        if (isNotBlank(s3Prefix)) {
            if (!s3Prefix.endsWith("/") || s3Prefix.startsWith("/") || s3Profile.contains(" "))
                throw new RuntimeException("S3 folder should be like `example/`, was: " + s3Prefix);
            System.out.println("S3 prefix: " + s3Prefix);
        }

        s3Threads = Integer.parseInt(cmd.getOptionValue("s3threads", "100"));
        System.out.println("S3 putter thread pool: " + s3Threads);

        awsRoleArn = StringUtils.trim(cmd.getOptionValue("awsRoleArn", null));
        if (isNotBlank(awsRoleArn)) System.out.println("AWS role to assume: " + awsRoleArn);

        skipGzip = cmd.hasOption("skipGzip");
        if (skipGzip) System.out.println("Skipping GZIP before put");

        skipPrefixHash = cmd.hasOption("skipPrefixHash");
        if (skipPrefixHash) System.out.println("Skipping hash of prefix (using raw value)");

        mongoUri = cmd.getOptionValue("c", "mongodb://localhost:27017");
        System.out.println("Connection string: " + mongoUri);

        String ns = cmd.getOptionValue("n", "test.s3mongo");
        String[] parts = ns.split("\\.");
        if (parts.length != 2) {
            System.err.println("namespace format is 'DATABASE.COLLECTION' ");
            System.exit(1);
        }

        databaseName = parts[0];
        collectionName = parts[1];

        fieldCount = Integer.parseInt(cmd.getOptionValue("f", "7"));

        drop = cmd.hasOption("d");
        testRebuild = cmd.hasOption("t");
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Region() {
        return s3Region;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public String getMongoUri() {
        return mongoUri;
    }
}