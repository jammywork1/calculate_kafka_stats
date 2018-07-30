# calculate_kafka_stats

It is a program for calculating the amount of data in the topics of the Kafka

among the collected data:
* date of first message
* date of last message
* number of days without messages (probably your software did not work on that day and did not write to the kafka)
* total number of messages in the topic
* total amount of messages in the topic
* 90, 95, 98 percents from daily routine and daily volume in the topic

topics are processed in parallel, using python asyncio stack

## usage

```
Usage: main.py [options]

Options:
  -h, --help            show this help message and exit
  -f OUTPUT_FILE_NAME, --output_file_name=OUTPUT_FILE_NAME
  -e REGEX_TOPIC_NAME, --regex_topic_name=REGEX_TOPIC_NAME
  -s BOOTSTRAP_SERVERS, --bootstrap_servers=BOOTSTRAP_SERVERS
  -t TOPIC, --topic=TOPIC
```

The -t and -s options can be arrays separated by commas.

Options -t and -e can't be used together.

If the -t option is empty, all topics in the kafka except '__consumer_offsets' will be used

Logs are written in log.log.

## example of usage

```
python3.6 src/main.py -s server1:9092,server2:9092,server3:9092 -t topic1,topic2 -f results.txt

python3.6 src/main.py -s server1:9092,server2:9092,server3:9092 -e topic.+ -f results.txt

python3.6 src/main.py -s server1:9092,server2:9092,server3:9092 -f results.txt
```

## example of run
 
![run.png](https://github.com/jammywork1/calculate_kafka_stats/raw/master/run.png "run.png")


## example of output

```
topic: test_topic
days_with_msg: 114
days_between_first_and_last_msgs: 114
days_offs: 0
first_date_msg: 2018-04-03
last_date_msg: 2018-07-25
size of all messages = 3573.71 mb
count of all messages = 5789447
errors read msgs: 0
percentile: 90: day_count = 72417, day_size = 44.53 mb
percentile: 95: day_count = 81293, day_size = 49.81 mb
percentile: 98: day_count = 86333, day_size = 53.18 mb
```