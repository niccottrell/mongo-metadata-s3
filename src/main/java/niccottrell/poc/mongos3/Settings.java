package niccottrell.poc.mongos3;

import org.apache.commons.cli.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Settings {

    protected final String s3Bucket;
    protected final String s3Region;
    protected final String s3Prefix;
    protected final int port;
    protected final String databaseName;
    protected final String collectionName;
    protected final boolean drop;
    public int fieldCount;
    public boolean testRebuild = false;
    public int numThreads = 4;
    public long reportTime = 10; // seconds between reports
    public long duration = 300; // seconds to run the test

    public Settings(String[] args) throws ParseException {

        CommandLineParser parser = new DefaultParser();

        Options cliopt = new Options();
        cliopt.addOption("b", "s3bucket", true, "S3 bucket name");
        cliopt.addOption("r", "s3region", true, "S3 region code");
        cliopt.addOption(null, "s3prefix", true, "S3 prefix path (optional, e.g. `/first_test` or ``)");
        cliopt.addOption(null, "port", true, "Mongo server port (optional)");
        cliopt.addOption("n", "namespace", true, "Mongo Namespace (default: `test.s3mongo`)");
        cliopt.addOption("d", false, "Drop collection at start of run");
        cliopt.addOption("f", "fieldCount", true, "Number of fields (default: 7)");
        cliopt.addOption("t", false, "Test rebuilding of data from S3");

        CommandLine cmd = parser.parse(cliopt, args);

        s3Bucket = cmd.getOptionValue("b");
        if (isBlank(s3Bucket)) throw new RuntimeException("No S3 bucket provided");

        s3Region = cmd.getOptionValue("r");
        if (isBlank(s3Region)) throw new RuntimeException("No S3 region provided");

        s3Prefix = cmd.getOptionValue("s3prefix", "");

        port = Integer.parseInt(cmd.getOptionValue("port", "27017"));

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

    public int getPort() {
        return port;
    }
}