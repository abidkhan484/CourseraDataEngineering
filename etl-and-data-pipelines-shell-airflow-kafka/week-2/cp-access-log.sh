

# create table if not exists access_log(timestamp timestamp,latitude float,longitude float,visitorid varchar(37));

# Extract data
echo "Extract data"
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz
gunzip web-server-access-log.txt.gz

cut -d"#" -f1-4 web-server-access-log.txt | tail +2 > extracted-access-log.txt

# Transform data
echo "Transform data"
tr "#" "," < extracted-access-log.txt > transformed-access-log.csv

# Load data
echo "Loading data"
echo "\c template1;\COPY access_log  FROM '/home/project/transformed-access-log.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=localhost

# Final output
echo "Final output"
echo '\c template1; \\SELECT * FROM access_log;' | psql --username=postgres --host=localhost

