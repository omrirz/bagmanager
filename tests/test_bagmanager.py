import tempfile
from pathlib import Path
import os

import numpy as np
import pytest
from pyrosenv import rosbag, rospy
from pyrosenv.sensor_msgs.point_cloud2 import create_cloud_xyz32
from pyrosenv.std_msgs.msg import Header


@pytest.fixture(scope='module')
def bag_file():
    np.random.seed(0)

    def _get_write_data(topic, stamp):
        frame_id = topic + '_frame'
        header = Header(stamp=stamp, frame_id=frame_id)
        msg = create_cloud_xyz32(header, np.random.rand(10, 3))
        t = header.stamp + rospy.Duration(np.random.uniform(1e-5, 1e-2))
        return topic, msg, t

    bag_path = Path(tempfile.NamedTemporaryFile(suffix='.bag', delete=False).name)
    with rosbag.Bag(bag_path, 'w') as bag:

        rospy.init_node('bag_manager_tester')
        start_time = rospy.Time.now()

        # the bag file start time is sometimes greater than the first msg rosbag_time. this should fix it.
        fix_start_time = _get_write_data(topic='fix_start_time', stamp=start_time)
        bag.write(*fix_start_time)

        curr_time = start_time + rospy.Duration(np.random.uniform(1e-2, 2))
        for i in range(10):
            curr_time += rospy.Duration(np.random.uniform(1e-2, 2))
            topic_1_tuple = _get_write_data(topic='topic_1', stamp=curr_time)
            bag.write(*topic_1_tuple)
            if i < 7:
                curr_time += rospy.Duration(np.random.uniform(1e-2, 2))
                topic_2_tuple = _get_write_data(topic='topic_2', stamp=curr_time)
                bag.write(*topic_2_tuple)

        # the bag file end time is sometimes less than the last msg rosbag_time. this should fix it.
        end_time = curr_time + rospy.Duration(np.random.uniform(1e-2, 2))
        fix_end_time = _get_write_data(topic='fix_end_time', stamp=end_time)
        bag.write(*fix_end_time)

    yield bag_path
    os.remove(bag_path)


def test_ctor(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    assert bool(bag_manager.bag_info) == True
    assert bool(bag_manager._topics_info_cache) == False


def test_topic_info_cache(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_1_info = bag_manager.get_topic_info('topic_1', get_header_time=False)
    assert bool(bag_manager._topics_info_cache) == True
    assert 'topic_1' in bag_manager._topics_info_cache
    assert bag_manager._topics_info_cache['topic_1']['msg_time_list_header'] is None
    assert 'topic_2' not in bag_manager._topics_info_cache

    topic_1_info_with_header_time = bag_manager.get_topic_info('topic_1', get_header_time=True)
    assert bool(bag_manager._topics_info_cache) == True
    assert 'topic_1' in bag_manager._topics_info_cache
    assert bag_manager._topics_info_cache['topic_1']['msg_time_list_header'] is not None
    assert 'topic_2' not in bag_manager._topics_info_cache

    topic_2_info_with_header_time = bag_manager.get_topic_info('topic_2', get_header_time=True)
    assert bool(bag_manager._topics_info_cache) == True
    assert 'topic_1' in bag_manager._topics_info_cache
    assert bag_manager._topics_info_cache['topic_1']['msg_time_list_header'] is not None
    assert 'topic_2' in bag_manager._topics_info_cache
    assert bag_manager._topics_info_cache['topic_2']['msg_time_list_header'] is not None


def test_get_closest_message_by_header_time(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_1_info = bag_manager.get_topic_info('topic_1', get_header_time=True)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=True)

    time_header = topic_1_info['msg_time_list_header'][4]
    msg_topic_2 = bag_manager.get_closest_message_by_header_time(topic='topic_2', time_header=time_header)
    msg_topic_2_idx = topic_2_info['msg_time_list_header'].index(msg_topic_2.header.stamp)
    assert msg_topic_2_idx == 4  # checked manually, since the seed is set to 0 this will always be true

    time_header = topic_2_info['msg_time_list_header'][0]
    msg_topic_1 = bag_manager.get_closest_message_by_header_time(topic='topic_1', time_header=time_header)
    msg_topic_1_idx = topic_1_info['msg_time_list_header'].index(msg_topic_1.header.stamp)
    assert msg_topic_1_idx == 0  # checked manually, since the seed is set to 0 this will always be true


def test_get_closest_message_by_rosbag_time(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_1_info = bag_manager.get_topic_info('topic_1', get_header_time=False)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=True)

    time_rosbag = topic_1_info['msg_time_list_rosbag'][9]
    msg_topic_2 = bag_manager.get_closest_message_by_rosbag_time(topic='topic_2', time_rosbag=time_rosbag)
    assert msg_topic_2.header.stamp == topic_2_info['msg_time_list_header'][-1]  # checked manually,
    # The last msg in topic_1 is sent after the last msg in topic_2 so we expect to get the last msg in topic_2


def test_get_message_by_index(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=True)

    with pytest.raises(IndexError) as e:
        msg_topic_2 = bag_manager.get_message_by_index(topic='topic_2', index=7)

    msg_topic_2 = bag_manager.get_message_by_index(topic='topic_2', index=6)
    assert msg_topic_2.header.stamp == topic_2_info['msg_time_list_header'][-1]  # checked manually,
    # We asked for the the msg with index 6 and we verify that is what we get

    msg_topic_2 = bag_manager.get_message_by_index(topic='topic_2', index=0)
    assert msg_topic_2.header.stamp == topic_2_info['msg_time_list_header'][0]  # checked manually,
    # We asked for the the msg with index 0 and we verify that is what we get


def test_get_message_count_topic_2_in_interval_all_times(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0]
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][-1]

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 7

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag)
    assert message_count == 7

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', end_time_rosbag=end_time_rosbag)
    assert message_count == 7

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2')
    assert message_count == 7


def test_get_message_count_topic_2_in_interval_first(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0]
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][0]

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 1


def test_get_message_count_topic_2_in_interval_last(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][-1]
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][-1]

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 1


def test_get_message_count_topic_2_in_interval_0_to_3(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0]
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][3]

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 4


def test_get_message_count_topic_2_in_interval_5_to_6(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][5]
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][6]

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 2


def test_get_message_count_topic_2_in_interval_around_msgs(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_2_info = bag_manager.get_topic_info('topic_2', get_header_time=False)
    epsilon = rospy.Duration(1e-8)

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][6] - epsilon
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][6] + epsilon

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 1

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - epsilon
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] + epsilon

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 1

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - epsilon
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][1] + epsilon

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 2

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - epsilon
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][-1] + epsilon

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 7

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - rospy.Duration(5)
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][-1] + rospy.Duration(5)

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 7

    start_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - epsilon
    end_time_rosbag = topic_2_info['msg_time_list_rosbag'][0] - epsilon

    message_count = bag_manager.get_message_count_in_interval(topics='topic_2', start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 0


def test_get_message_count_all_topics_all_times(bag_file):
    from bagmanager import BagManager
    bag_manager = BagManager(bag_file=bag_file)
    topic_1_info = bag_manager.get_topic_info('topic_1', get_header_time=False)
    epsilon = rospy.Duration(1e-8)

    start_time_rosbag = topic_1_info['msg_time_list_rosbag'][0] - epsilon
    end_time_rosbag = topic_1_info['msg_time_list_rosbag'][9] + epsilon

    message_count = bag_manager.get_message_count_in_interval(topics=['topic_1', 'topic_2'],
                                                              start_time_rosbag=start_time_rosbag,
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 17

    message_count = bag_manager.get_message_count_in_interval(topics=['topic_1', 'topic_2'],
                                                              start_time_rosbag=start_time_rosbag)
    assert message_count == 17

    message_count = bag_manager.get_message_count_in_interval(topics=['topic_1', 'topic_2'],
                                                              end_time_rosbag=end_time_rosbag)
    assert message_count == 17

    message_count = bag_manager.get_message_count_in_interval(topics=['topic_1', 'topic_2'])
    assert message_count == 17
