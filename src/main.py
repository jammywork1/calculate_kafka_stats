import logging
import re
from datetime import datetime
import numpy as np
import asyncio
from optparse import OptionParser
from aiokafka import AIOKafkaConsumer, TopicPartition
from kafka.errors import ConnectionError, KafkaError
from tqdm import tqdm
import sys
from datetime import date
import json
from uuid import uuid4


async def get_all_topics(kafka_servers):
    try:
        consumer = AIOKafkaConsumer(
            loop=loop,
            bootstrap_servers=kafka_servers)
        await consumer.start()
        topics_set = await consumer.topics()
        return tuple(topic for topic in topics_set if topic != "__consumer_offsets")
    finally:
        await close_consumer(consumer)


async def open_consumer(kafka_server, topic):
    consumer = AIOKafkaConsumer(
        topic, loop=loop,
        bootstrap_servers=kafka_server,
        group_id='python_kafka_size_calc_'+str(uuid4()))
    await consumer.start()
    return consumer


async def get_offsets_scope_of_topic_and_set_to_start(consumer, topic):
    partition_set = consumer.partitions_for_topic(topic)
    logging.info((topic, partition_set))
    await consumer.seek_to_end()
    last_offset = None
    for partition in partition_set:
        tp = TopicPartition(topic, partition)
        offset = await consumer.position(tp)
        if (offset and not last_offset) or (offset and last_offset and offset > last_offset):
            last_offset = offset
        logging.info((topic, partition, last_offset))

    await consumer.seek_to_beginning()
    first_offset = None
    for partition in partition_set:
        tp = TopicPartition(topic, partition)
        offset = await consumer.position(tp)
        if (offset and not first_offset) or (offset and last_offset and offset < first_offset):
            first_offset = offset
        logging.info((topic, partition, first_offset))
    if last_offset and not first_offset:
        first_offset = 0
    return first_offset, last_offset


async def close_consumer(consumer):
    await consumer.stop()


async def get_next_message_data(consumer):
    msg = await consumer.getone()
    if not msg.timestamp:
        try:
            recivied_date = datetime.fromtimestamp(json.loads(msg.value.decode('utf-8'))['createdAt'])
        except: 
            recivied_date = date(1900, 1, 1)
    else:
        recivied = datetime.fromtimestamp(msg.timestamp / 1000.)
        recivied_date = recivied.date()
    size = len(msg.value)
    return recivied_date, size, msg.offset


def update_data_dict(data_dict, recivied_date, size):
    at_day_value = data_dict.get(recivied_date)
    if at_day_value:
        msg_count, msgs_sizes = at_day_value
        data_dict[recivied_date] = (msg_count + 1, msgs_sizes + size)
    else:
        data_dict[recivied_date] = (1, size)


async def fetch_topic_data(kafka_server, topic, index):
    consumer = await open_consumer(kafka_server, topic)
    first_offset, last_offset = await get_offsets_scope_of_topic_and_set_to_start(consumer, topic)
    logging.info((topic, first_offset, last_offset))
    data_dict = {}
    msgs_sizes = 0
    errors_msgs_count = 0
    readed_msgs_count = 0

    with tqdm(total=last_offset - first_offset, desc=f'{topic}', position=index, file=sys.stdout) as pbar:
        try:
            while True:
                try:
                    recivied_date, size, offset = await get_next_message_data(consumer)
                    update_data_dict(data_dict, recivied_date, size)
                    msgs_sizes += size
                    readed_msgs_count += 1
                    if offset >= last_offset - 1:
                        break
                except KafkaError:
                    logging.error(f'topic {topic} fail read message. {repr(e)}')
                    break
                except Exception as e:
                    errors_msgs_count += 1
                    logging.error(f'topic {topic} fail read message. {repr(e)}')
                finally:
                    pbar.update()
        finally:
            await close_consumer(consumer)
    if errors_msgs_count > 0 and (readed_msgs_count + errors_msgs_count) / errors_msgs_count > 2:
        raise Exception("more 2% messages unread")
    return data_dict, msgs_sizes / 1024. / 1024., readed_msgs_count, errors_msgs_count


async def calculate_topic(kafka_server, topic, index, writer):
    logging.info(f'start calculate topic: {topic}')
    try:
        data_dict, msgs_sizes, readed_msgs_count, errors_msgs_count = await fetch_topic_data(kafka_server, topic, index)

        percentiles = (90, 95, 98)
        percentiles_results = {}
        for percentile in percentiles:
            p_count = np.percentile(list(map(lambda x: x[0], data_dict.values())), percentile)
            p_size = np.percentile(list(map(lambda x: x[1], data_dict.values())), percentile) / 1024. / 1024.
            percentiles_results[percentile] = (p_count, p_size)

        sorted_dates = sorted(data_dict.keys())

        first_date_msg, last_date_msg = sorted_dates[0], sorted_dates[-1]
        days_with_msg = len(sorted_dates)
        days_between_first_and_last_msgs = (last_date_msg - first_date_msg).days + 1
        days_offs = days_between_first_and_last_msgs - days_with_msg
        days_info = f'days_with_msg: {days_with_msg}\n' \
                    f'days_between_first_and_last_msgs: {days_between_first_and_last_msgs}\n' \
                    f'days_offs: {days_offs}\n' \
                    f'first_date_msg: {first_date_msg}\n' \
                    f'last_date_msg: {last_date_msg}\n'

        percentiles_logs = [f'percentile: {p_value}: day_count = {p_count:.0f}, day_size = {p_size:.2f} mb' for
                            p_value, (p_count, p_size) in percentiles_results.items()]
        writer.write(f'topic: {topic}\n' +
              days_info +
              f'size of all messages = {msgs_sizes:.2f} mb\n' +
              f'count of all messages = {readed_msgs_count + errors_msgs_count}\n'
              f'errors read msgs: {errors_msgs_count}\n' +
              '\n'.join(percentiles_logs) + '\n\n')
    except Exception as e:
        writer.write(f'topic {topic} calculate fail. {repr(e)}')


async def is_topic_empty(kafka_server, topic):
    consumer = await open_consumer(kafka_server, topic)
    try:
        first_offset, last_offset = await get_offsets_scope_of_topic_and_set_to_start(consumer, topic)
        if not last_offset or first_offset == last_offset:
            logging.error(f'topic `{topic} is empty`')
            return True
        return False
    finally:
        await close_consumer(consumer)


async def main(args):
    logging.info(args.topic)
    logging.info(args.bootstrap_servers)

    if not args.output_file_name:
        raise Exception('option cant be null')
    if not args.bootstrap_servers:
        raise Exception('option cant be null')
    if args.topic and args.regex_topic_name:
        raise Exception('only one option can be set')

    all_topics = await get_all_topics(args.bootstrap_servers)

    topics = args.topic
    if not topics and not args.regex_topic_name:
        topics = all_topics
    elif args.regex_topic_name:
        topics = tuple(topic for topic in all_topics if re.match(args.regex_topic_name, topic))

    different_topics = set(topics) - set(all_topics)
    if len(different_topics) > 0:
        logging.error(different_topics)
        raise Exception(f'topic not found {different_topics}')

    topics = [topic for topic in topics if not await is_topic_empty(args.bootstrap_servers, topic)]
    tasks = []
    with open(args.output_file_name, 'w') as writer:
        for index, topic in enumerate(sorted(topics)):
            tasks.append(calculate_topic(args.bootstrap_servers, topic, index, writer))
        await asyncio.gather(*tasks, return_exceptions=True)
        print('\n' * len(topics))


def get_comma_separated_args(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))


logging.basicConfig(filename='log.log', level=logging.INFO)
loop = asyncio.get_event_loop()

parser = OptionParser()
parser.add_option("-f", "--output_file_name", type='string')
parser.add_option("-e", "--regex_topic_name", type='string')
parser.add_option("-s", "--bootstrap_servers", type='string',
                  action='callback',
                  callback=get_comma_separated_args)
parser.add_option("-t", "--topic", type='string',
                  action='callback',
                  callback=get_comma_separated_args)

(options, args) = parser.parse_args()

loop.run_until_complete(main(options))
