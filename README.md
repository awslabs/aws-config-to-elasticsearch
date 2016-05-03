Import your AWS Config Snapshots into ElasticSearch
===================================================

### What does this app do?
It will ingest all of your AWS Config Snapshots into ElasticSearch for further analysis with Kibana

### Getting the code
```
git clone --depth 1 <repo_url>
```

### The code
#### Prerequisites
* Python 2.7
* An ELK stack, up and running
* Install the required packages. The requirements.txt file is included with this repo.
```
pip install -r ./requirements.txt
```

#### The command
```bash
./esingest.py
usage: esingest.py [-h] [--region REGION] --destination DESTINATION [--verbose]

```

1. Let's say that you have your ElasticSearch node running on localhost:9200 and you want to import only your us-east-1 snapshot, then you'd run the following command:
```bash
./esingest.py -d localhost:9200 -r us-east-1
```

2. If you want to import Snapshots from all of your AWS Config-enabled regions, run the command without the '-r' parameter:
```bash
./esingest.py -d localhost:9200
```
3. To run the command in verbose mode, use the -v parameter
```bash
./esingest.py -v -d localhost:9200 -r us-east-1
```

### Cleanup

####DON'T RUN THESE COMMANDS IF YOU DON'T WANT TO LOSE EVERYTHING IN YOUR ELASTICSEARCH NODE!

#####_THIS COMMAND WILL ERASE EVERYTHING FROM YOUR ES NODE --- BE CAREFUL BEFORE RUNNING_
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