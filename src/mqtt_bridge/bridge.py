from abc import ABCMeta
from typing import Optional, Type, Dict, Union

import inject
import paho.mqtt.client as mqtt
import rospy

import time
from std_msgs.msg import Header
from benchmarking_suite.msg import TimeStamped

from .util import lookup_object, extract_values, populate_instance


def walltimeNow() -> rospy.Time:
    t_ns = time.time_ns()
    t = rospy.Time()
    t.secs = int(str(t_ns)[:-9])
    t.nsecs = int(str(t_ns)[-9:])
    return t


def createTimestamp(time: rospy.Time, header: Header) -> TimeStamped:
    ts = TimeStamped()
    ts.time = time
    ts.header = header
    return ts


def create_bridge(factory: Union[str, "Bridge"], msg_type: Union[str, Type[rospy.Message]], topic_from: str,
                  topic_to: str, frequency: Optional[float] = None, **kwargs) -> "Bridge":
    """ generate bridge instance using factory callable and arguments. if `factory` or `meg_type` is provided as string,
     this function will convert it to a corresponding object.
    """
    if isinstance(factory, str):
        factory = lookup_object(factory)
    if not issubclass(factory, Bridge):
        raise ValueError("factory should be Bridge subclass")
    if isinstance(msg_type, str):
        msg_type = lookup_object(msg_type)
    if not issubclass(msg_type, rospy.Message):
        raise TypeError(
            "msg_type should be rospy.Message instance or its string"
            "reprensentation")
    return factory(
        topic_from=topic_from, topic_to=topic_to, msg_type=msg_type, frequency=frequency, **kwargs)


class Bridge(object, metaclass=ABCMeta):
    """ Bridge base class """
    _mqtt_client = inject.attr(mqtt.Client)
    _serialize = inject.attr('serializer')
    _deserialize = inject.attr('deserializer')
    _extract_private_path = inject.attr('mqtt_private_path_extractor')


class RosToMqttBridge(Bridge):
    """ Bridge from ROS topic to MQTT

    bridge ROS messages on `topic_from` to MQTT topic `topic_to`. expect `msg_type` ROS message type.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: rospy.Message, frequency: Optional[float] = None, timestamp_topic_prefix: Optional[str] = None):
        self._topic_from = topic_from
        self._topic_to = self._extract_private_path(topic_to)
        self._last_published = rospy.get_time()
        self._interval = 0 if frequency is None else 1.0 / frequency
        rospy.Subscriber(topic_from, msg_type, self._callback_ros)

        # create ROS timestamp publishers
        self.timestamp_topic_prefix = timestamp_topic_prefix
        if timestamp_topic_prefix is not None:
            self.ts_in_pub = rospy.Publisher(timestamp_topic_prefix + "/in", TimeStamped, queue_size=10)
            self.ts_out_pub = rospy.Publisher(timestamp_topic_prefix + "/out", TimeStamped, queue_size=10)

    def _callback_ros(self, msg: rospy.Message):

        self.t_in = walltimeNow()
        rospy.logdebug("ROS received from {}".format(self._topic_from))
        now = rospy.get_time()
        if now - self._last_published >= self._interval:
            self._publish(msg)
            self._last_published = now
        self.t_out = walltimeNow()

        # publish timestamps
        if self.timestamp_topic_prefix is not None:
            ts_in = createTimestamp(self.t_in, msg.header)
            ts_out = createTimestamp(self.t_out, msg.header)
            self.ts_in_pub.publish(ts_in)
            self.ts_out_pub.publish(ts_out)

    def _publish(self, msg: rospy.Message):
        payload = self._serialize(extract_values(msg))
        self._mqtt_client.publish(topic=self._topic_to, payload=payload)


class MqttToRosBridge(Bridge):
    """ Bridge from MQTT to ROS topic

    bridge MQTT messages on `topic_from` to ROS topic `topic_to`. MQTT messages will be converted to `msg_type`.
    """

    def __init__(self, topic_from: str, topic_to: str, msg_type: Type[rospy.Message],
                 frequency: Optional[float] = None, queue_size: int = 10, timestamp_topic_prefix: Optional[str] = None):
        self._topic_from = self._extract_private_path(topic_from)
        self._topic_to = topic_to
        self._msg_type = msg_type
        self._queue_size = queue_size
        self._last_published = rospy.get_time()
        self._interval = None if frequency is None else 1.0 / frequency
        # Adding the correct topic to subscribe to
        self._mqtt_client.subscribe(self._topic_from)
        self._mqtt_client.message_callback_add(self._topic_from, self._callback_mqtt)
        self._publisher = rospy.Publisher(
            self._topic_to, self._msg_type, queue_size=self._queue_size)

        # create ROS timestamp publishers
        self.timestamp_topic_prefix = timestamp_topic_prefix
        if timestamp_topic_prefix is not None:
            self.ts_in_pub = rospy.Publisher(timestamp_topic_prefix + "/in", TimeStamped, queue_size=10)
            self.ts_out_pub = rospy.Publisher(timestamp_topic_prefix + "/out", TimeStamped, queue_size=10)

    def _callback_mqtt(self, client: mqtt.Client, userdata: Dict, mqtt_msg: mqtt.MQTTMessage):
        """ callback from MQTT """

        self.t_in = walltimeNow()
        rospy.logdebug("MQTT received from {}".format(mqtt_msg.topic))
        now = rospy.get_time()

        if self._interval is None or now - self._last_published >= self._interval:
            try:
                ros_msg = self._create_ros_message(mqtt_msg)
                self._publisher.publish(ros_msg)
                self.t_out = walltimeNow()
                self._last_published = now

                # publish timestamps
                if self.timestamp_topic_prefix is not None:
                    ts_in = createTimestamp(self.t_in, ros_msg.header)
                    ts_out = createTimestamp(self.t_out, ros_msg.header)
                    self.ts_in_pub.publish(ts_in)
                    self.ts_out_pub.publish(ts_out)

            except Exception as e:
                rospy.logerr(e)

    def _create_ros_message(self, mqtt_msg: mqtt.MQTTMessage) -> rospy.Message:
        """ create ROS message from MQTT payload """
        # Hack to enable both, messagepack and json deserialization.
        if self._serialize.__name__ == "packb":
            msg_dict = self._deserialize(mqtt_msg.payload, raw=False)
        else:
            msg_dict = self._deserialize(mqtt_msg.payload)
        return populate_instance(msg_dict, self._msg_type())


__all__ = ['create_bridge', 'Bridge', 'RosToMqttBridge', 'MqttToRosBridge']
