from typing import Dict, Iterable, Union, Optional
import subprocess
import pathlib

import numpy as np
import yaml
from pyrosenv import rosbag, rospy
from pyrosenv import genpy

TimeLike = Union[float, rospy.Time, genpy.Time]
PathLike = Union[str, pathlib.Path]
RosMessage = genpy.Message

__all__ = ['BagManager']


"""
In this module there are two types of time for each msg:
1. time_rosbag: the time that the message was recorded into the bag file.
    for topic, msg, t in self.bag.read_messages():
        print(t)
2. time_header: the time from the msg header timestamp.
    for topic, msg, t in self.bag.read_messages():
        print(msg.header.stamp)
"""


class BagManagerException(Exception):
    pass


class BagManager:
    def __init__(self, bag_file: PathLike):
        self.bag = rosbag.Bag(bag_file)
        self.bag_info = self._get_bag_info(bag_file)
        self._topics_info_cache = {}

    @staticmethod
    def _get_bag_info(bag_file: PathLike) -> Dict:
        """ Return a dict with information about the given bag file"""
        bag_file = str(bag_file)
        bag_info = yaml.safe_load(subprocess.Popen(['rosbag', 'info', '--yaml', bag_file],
                                                   stdout=subprocess.PIPE).communicate()[0])
        return bag_info

    def get_topic_info(self, topic: str, get_header_time: bool = False) -> Dict:
        """
        Return a dict with info about the given topic.
        with get_header_time=True the function may run a long time
        but returns also a list of the messages times from the messages headers.
        """
        def _cache_topic_info_without_msg_time_list_header():
            topic_tuple = self.bag.get_type_and_topic_info().topics[topic]
            connections = self.bag._get_connections([topic], connection_filter=None)
            entry_gen = self.bag._get_entries(connections, start_time=None, end_time=None)
            msg_time_list_rosbag = [entry.time for entry in entry_gen]
            topic_info = {'topic': topic, 'message_count': topic_tuple.message_count,
                          'message_type': topic_tuple.msg_type, 'frequency': topic_tuple.frequency,
                          'msg_time_list_header': None, 'msg_time_list_rosbag': msg_time_list_rosbag}
            return topic_info

        if topic in self._topics_info_cache:
            topic_info = self._topics_info_cache[topic]
        else:
            topic_info = _cache_topic_info_without_msg_time_list_header()
            self._topics_info_cache[topic] = topic_info

        if get_header_time:
            if topic_info['msg_time_list_header'] is None:
                try:
                    msg_time_list_header = [msg.header.stamp for _, msg, _ in self.bag.read_messages(topics=[topic])]
                except AttributeError:
                    msg_time_list_header = BagManagerException("Some msg doesn't have header timestamp")
                self._topics_info_cache[topic]['msg_time_list_header'] = msg_time_list_header

        return topic_info
        # BagManagerException("Call get_topic_info() with get_header_time=True to get the headers stamps")

    def get_closest_message_by_header_time(self, topic: str, time_header: TimeLike) -> RosMessage:
        """ Returns a message from the given topic with header timestamp closest to time_header """
        if not isinstance(time_header, rospy.Time) and not isinstance(time_header, genpy.Time):
            time_header = rospy.Time(time_header)
        info = self.get_topic_info(topic=topic, get_header_time=True)
        argmin = np.argmin([abs(t - time_header) for t in info['msg_time_list_header']])
        matching_msg_time_rosbag = info['msg_time_list_rosbag'][argmin]
        msg = [msg for _, msg, _ in self.bag.read_messages(topics=[topic],
                                                           start_time=matching_msg_time_rosbag,
                                                           end_time=matching_msg_time_rosbag)][0]
        return msg

    def get_closest_message_by_rosbag_time(self, topic: str, time_rosbag: TimeLike) -> RosMessage:
        """ Returns a message from the given topic with rosbag timestamp closest to time_rosbag """
        if not isinstance(time_rosbag, rospy.Time) and not isinstance(time_rosbag, genpy.Time):
            time_rosbag = rospy.Time(time_rosbag)
        info = self.get_topic_info(topic=topic, get_header_time=False)
        argmin = np.argmin([abs(t - time_rosbag) for t in info['msg_time_list_rosbag']])
        matching_msg_time_rosbag = info['msg_time_list_rosbag'][argmin]
        msg = [msg for _, msg, _ in self.bag.read_messages(topics=[topic],
                                                           start_time=matching_msg_time_rosbag,
                                                           end_time=matching_msg_time_rosbag)][0]
        return msg

    def get_message_by_index(self, topic: str, index: int) -> RosMessage:
        """ Returns a message from the given topic by the given index """
        info = self.get_topic_info(topic=topic, get_header_time=False)
        msg_time_rosbag = info['msg_time_list_rosbag'][index]
        msg = [msg for _, msg, _ in self.bag.read_messages(topics=[topic],
                                                           start_time=msg_time_rosbag,
                                                           end_time=msg_time_rosbag)][0]
        return msg

    def get_message_count_in_interval(self, topics: Optional[Iterable[str]] = None,
                                      start_time_rosbag: Optional[TimeLike] = None,
                                      end_time_rosbag: Optional[TimeLike] = None) -> int:
        """
        Count the number of messages in topics in the interval: [start_time_rosbag, end_time_rosbag]
        if topics is None it will count in all topics.
        if start_time_rosbag is None it will count from the bag file start
        if end_time_rosbag is None it will count to the bag file end
        """
        if topics is None:
            topics = [t['topic'] for t in self.bag_info['topics']]
        if isinstance(topics, str):
            topics = [topics]
        if start_time_rosbag is None:
            start_time_rosbag = self.bag_info['start']
        if end_time_rosbag is None:
            end_time_rosbag = self.bag_info['end']

        if not isinstance(start_time_rosbag, rospy.Time) and not isinstance(start_time_rosbag, genpy.Time):
            start_time_rosbag = rospy.Time(start_time_rosbag)
        if not isinstance(end_time_rosbag, rospy.Time) and not isinstance(end_time_rosbag, genpy.Time):
            end_time_rosbag = rospy.Time(end_time_rosbag)

        message_count = 0
        for topic in topics:
            msg_list = self.get_topic_info(topic=topic, get_header_time=False)['msg_time_list_rosbag']
            number_of_msgs_in_interval = (np.searchsorted(msg_list, end_time_rosbag, side='right')
                                          - np.searchsorted(msg_list, start_time_rosbag, side='left'))
                                          
            message_count += number_of_msgs_in_interval
        return message_count

    def __repr__(self):
        return f"BagManager [ path: {self.bag_info['path']} ] [ duration: {self.bag_info['duration']:.1f} sec ] [ messages: {self.bag_info['messages']} ]"

    def __str__(self):
        return self.bag.__str__()
