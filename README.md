# bagmanager

A thin wrapper around rosbag.Bag with some convenient methods

![alt text](https://img.icons8.com/doodle/344/bag-front-view.png "bagmanager logo")

## Installation

```console
pip install bagmanager
```

## Usage

```python
from bagmanager import BagManager
bag_file = '/path/to/bag/file.bag'
bag_manager = BagManager(bag_file=bag_file)

# info about the bag file
print(bag_manager)

# get information about the topic
topic_info = bag_manager.get_topic_info(topic='some_topic', get_header_time=True)
# get_header_time=True is costly but returns a list of timestamps from the messages headers.

# get the timestamp from the message header of the 3rd message
time_header = topic_info['msg_time_list_header'][2]

# get the timestamp from the bag of the 1st message
time_rosbag = topic_info['msg_time_list_rosbag'][0]

# get the number of messages in the topic
number_of_messages_in_topic = topic_info['message_count']

# get the message type of the topic
message_type = topic_info['message_type']

# get the frequency of messages in the topic
frequency = topic_info['frequency']


# get the closest msg from 'another_topic' to a message from 'some_topic'by its 3rd message **header** time
msg = bag_manager.get_closest_message_by_header_time(topic='another_topic', time_header=time_header)

# get the closest msg from 'another_topic' to a message from 'some_topic'by its 1st message **rosbag** time
msg = bag_manager.get_closest_message_by_rosbag_time(topic='another_topic', time_rosbag=time_rosbag)

# get a msg from 'some_topic' by index
msg = bag_manager.get_message_by_index(topic='some_topic', index=number_of_messages_in_topic-1)


# get the number of messages from topics in an interval of times
start_time_rosbag = topic_info['msg_time_list_rosbag'][2]
end_time_rosbag = topic_info['msg_time_list_rosbag'][7]
message_count = bag_manager.get_message_count_in_interval(topics=['some_topic', 'another_topic'], start_time_rosbag=start_time_rosbag, end_time_rosbag=end_time_rosbag)
```

## Development

Create a venv (recommended)

```console
git clone https://github.com/omrirz/bagmanager.git
python -m pip install -r requirements.txt
```

## Test

```console
python -m pytest
```
