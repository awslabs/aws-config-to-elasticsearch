Import your AWS Config Snapshots into ElasticSearch
===================================================

# What does this app do?
It will ingest all of your AWS Config Snapshots into ElasticSearch for further analysis with Kibana

# Getting the code
```
git clone --depth 1 <the url for this repo>
```

# Running the code
Install the following two packages (those are the only ones I had to install while writing the code):
```python
pip install boto3
pip install gzip
```
And here's the command:
```bash
./ESingest.py
usage: ESingest.py [-h] [--region REGION] --destination DESTINATION
                   [--verbose]
ESingest.py: error: argument --destination/-d is required
```

Let's say that you have your ElasticSearch node running on 8.8.8.8:9200 and you want to import only your us-east-1 snapshot, then you'd run the following command:
```bash
./ESingest.py -v -d 8.8.8.8:9200 -r us-east-1
```

If you want to import Snapshots from all of your AWS Config-enabled regions, run the command without the '-r' parameter:
```bash
./ESingest.py -v -d 8.8.8.8:9200
```
# Cleanup -- DON'T RUN THESE COMMANDS IF YOU DON'T WANT TO LOSE EVERYTHING IN YOUR ELASTICSEARCH NODE!!!!
__THIS COMMAND WILL ERASE EVERYTHING FROM YOUR ES NODE --- BE CAREFUL BEFORE RUNNING__
```bash
curl -XDELETE localhost:9200/_all
```

In order to avoid losing all of your data, you can just iterate over all of your indexes and delete them that way. The below command will print out all of your indexes that contain 'aws::'. You can then run a DELETE on just these indexes.
```bash
curl 'localhost:9200/_cat/indices' | awk '{print $3}' | grep "aws::"
```

Also delete the template which allows for creationg of a 'raw' string value alongside every 'analyzed' one
```bash
curl -XDELETE localhost:9200/_template/configservice
```