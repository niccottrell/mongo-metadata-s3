#
# Set up PoC with MongoDB on same EC2 host
#

# Configure mongo repo
sudo bash -c "cat >> /etc/yum.repos.d/mongodb-org-3.6.repo" << EOL
[mongodb-org-3.6]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/3.6/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-3.6.asc
EOL

# Install mongo server
sudo yum install -y mongodb-org

# Start mongod
sudo service mongod start

# Install Java
sudo yum install -y java-1.8.0-openjdk

# Download jar
wget https://github.com/niccottrell/mongo-metadata-s3/releases/download/v1.0.2/mongos3-1.0-jar-with-dependencies.jar

# Run the POC
java -jar mongos3-1.0-jar-with-dependencies.jar -r <REGION> -b <BUCKET> --fieldCount 2000