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