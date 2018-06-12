# S3 Metadata Proof-of-Concept

This demo creates dummy MongoDB documents with a userId and some other payload. 

The full document is stored in S3 and only certain metadata fields are persisted to MongoDB.

Using the fields from the document in MongoDB is it possible to load a Document object from S3.
The demo also shows rebuilding documents in MongoDB from S3.

## Use case

The problem was storing large documents (e.g. > 100KB) when we are writing millions of documents per day. 
This makes the total data size grow quickly - leading to the need to shard early. 

Furthermore, we add the requirement to store documents for several years.

However, only a small subset of fields need to be queried via MongoDB, so we are exploring the possibility to store the full payload in S3.

## Documents generated

The demo will generate documents like:

```
{
  "_id": { "$oid": "5afd85cc198ef4b1b7ea024e" },
  "userId": 723,
  "name": "MongoDB",
  "type": "database",
  "count": 811,
  "versions": ["v3.2", "v3.0", "v2.6"],
  "data": "kl8QGAFArghImN9chteLA36gaHMTgF6zDFuipLqCoCBmz8IfBA7m4qFdP69JlO3v",
  "info": {
    "x": 203,
    "y": 102
  }
}
```

However only a subset will be stored in MongoDB like:


```
{
  "_id": { "$oid": "5afd85cc198ef4b1b7ea024e" },
  "userId": 723,
  "name": "MongoDB",
  "type": "database",
  "count": 811,
}
```

From this meta-data it is possible to load the full document from S3 at `{userId}/{_id}.json` in this case `723/5afd85cc198ef4b1b7ea024e.json` .

By using the `userId` as a prefix we can generate S3 keys with randomly-distributed prefixes for improved S3 write performance.

## Indexing

The metadata fields (here `name`, `type`, `count`) can be indexed in MongoDB making it performant to query millions of documents, then load data from S3 only when needed.

## Requirements

This demo assumes that there is a local MongoDB server running and that you have valid AWS credentials setup in your environment (e.g. in `~/.aws/config`)

You will need to choose a new `s3bucketName` field before compiling. 

# Running

Build with `mvn package`

Configure your AWS creditials for the current user.

Then run with `java -jar target/mongos3-1.0-SNAPSHOT-jar-with-dependencies.jar -r <S3REGION> -b <S3BUCKET>`

## Usage

```
usage: 
  -b,--s3bucket <arg>     S3 bucket name
  -c,--uri <arg>          MongoDB connection details (default 'mongodb://localhost:27017' )
  -d,--cleanup            Drop collection and S3 data at start of run
  -f,--fieldCount <arg>   Number of fields (default: 7)
  -h,--help               Print help only
  -n,--namespace <arg>    MongoDB namespace (default: `test.s3mongo`)
  -p,--s3profile <arg>    S3 profile to use
  -r,--s3region <arg>     S3 region code
     --s3prefix <arg>     S3 prefix path (optional, e.g. `first_test/` or ``)
     --s3threads <arg>    The max number of parallel S3 putObject threads, default 100
     --skipGzip           Store in S3 as JSON (not .json.gz)
     --skipPrefixHash     Create S3 path based on raw field value
  -t                      Test rebuilding of data from S3
```