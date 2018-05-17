# S3 Metadata Proof-of-Concept

This demo creates dummy MongoDB documents with a userId and some other payload. 
The full document is stored in S3 and only certain metadata fields are persisted to MongoDB.
Using the fields from the document in MongoDB is it possible to load a Document object from S3.
The demo also shows rebuilding documents in MongoDB from S3.