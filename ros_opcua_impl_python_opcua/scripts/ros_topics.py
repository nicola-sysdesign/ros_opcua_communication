#!/usr/bin/env python

# Thanks to:
# https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_topic/src/rqt_topic/topic_widget.py
import random
import numpy

import rospy
import roslib
import roslib.message
from opcua import ua, uamethod

import ros_server
import ros_actions
import rostopic


class OpcUaROSTopic:

    def __init__(self, server, parent, idx, topic_name, topic_type):
        self.server = server
        self.parent = self.recursive_create_objects(topic_name, idx, parent)
        self.type_name = topic_type
        self.name = topic_name
        self._nodes = {}
        self.idx = idx

        self.message_class = None
        try:
            self.message_class = roslib.message.get_message_class(topic_type)
            self.message_instance = self.message_class()

        except rospy.ROSException:
            self.message_class = None
            rospy.logfatal("Couldn't find message class for type " + topic_type)

        self._recursive_create_items(self.parent, idx, topic_name, topic_type, self.message_instance, True)

        self._subscriber = rospy.Subscriber(self.name, self.message_class, self.message_callback)
        self._publisher = rospy.Publisher(self.name, self.message_class, queue_size=1)
        rospy.loginfo("Created ROS Topic with name: " + str(self.name))


    def _recursive_create_items(self, parent, idx, topic_name, type_name, message, top_level=False):
        topic_text = topic_name.split('/')[-1]
        if '[' in topic_text:
            topic_text = topic_text[topic_text.index('['):]

        # This here are 'complex datatypes'
        if hasattr(message, '__slots__') and hasattr(message, '_slot_types'):
            complex_type = True
            new_node = parent.add_object(ua.NodeId(topic_name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                                         ua.QualifiedName(topic_name, parent.nodeid.NamespaceIndex))
            new_node.add_property(ua.NodeId(topic_name + ".Type", idx),
                                  ua.QualifiedName("Type", parent.nodeid.NamespaceIndex), type_name)
            if top_level:
                new_node.add_method(ua.NodeId(topic_name + ".Update", parent.nodeid.NamespaceIndex),
                                    ua.QualifiedName("Update", parent.nodeid.NamespaceIndex),
                                    self.opcua_update_callback, [], [])
            for slot_name, type_name_child in zip(message.__slots__, message._slot_types):
                self._recursive_create_items(new_node, idx, topic_name + '/' + slot_name, type_name_child, getattr(message, slot_name))
            self._nodes[topic_name] = new_node
        else:
            # This are arrays
            base_type_str, array_size = extract_array_info(type_name)
            try:
                base_instance = roslib.message.get_message_class(base_type_str)()
            except (ValueError, TypeError):
                base_instance = None

            if array_size is not None and hasattr(base_instance, '__slots__'):
                for index in range(array_size):
                    self._recursive_create_items(parent, idx, topic_name + '[%d]' % index, base_type_str, base_instance)
            else:
                node_variable = create_topic_variable(parent, idx, topic_name, topic_text, type_name)
                self._nodes[topic_name] = node_variable

        if topic_name in self._nodes and self._nodes[topic_name].get_node_class() == ua.NodeClass.Variable:
            self._nodes[topic_name].set_writable(True)
        return


    def message_callback(self, msg):
        self.update_value(self.name, msg)


    @uamethod
    def opcua_update_callback(self, parent):
        try:
            for nodeName in self._nodes:
                child = self._nodes[nodeName]
                name = child.get_display_name().Text
                if hasattr(self.message_instance, name):
                    if child.get_node_class() == ua.NodeClass.Variable:
                        setattr(self.message_instance, name,
                                correct_type(child, type(getattr(self.message_instance, name))))
                    elif child.get_node_class == ua.NodeClass.Object:
                        setattr(self.message_instance, name, self.create_message_instance(child))
            self._publisher.publish(self.message_instance)
        except rospy.ROSException as e:
            rospy.logerr("Error when updating node " + self.name, e)
            self.server.server.delete_nodes([self.parent])


    def update_value(self, topic_name, msg):

        if hasattr(msg, '__slots__') and hasattr(msg, '_slot_types'):
            # complex type
            for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
                self.update_value(topic_name + '/' + slot_name, getattr(msg, slot_name))
            return

        if type(msg) in (list, tuple):
            if len(msg) > 0 and hasattr(msg[0], '__slots__'):
                # complex type array
                for index, slot in enumerate(msg):
                    if topic_name + '[%d]' % index in self._nodes:
                        self.update_value(topic_name + '[%d]' % index, slot)
                    else:
                        if topic_name in self._nodes:
                            base_type_str, array_size = extract_array_info(self._nodes[topic_name].text(self.type_name))
                            self._recursive_create_items(self._nodes[topic_name], topic_name + '[%d]' % index, base_type_str, slot, None)
                # remove obsolete children
                if topic_name in self._nodes:
                    if len(message) < len(self._nodes[topic_name].get_children()):
                        for i in range(len(msg), self._nodes[topic_name].childCount()):
                            item_topic_name = topic_name + '[%d]' % i
                            self.recursive_delete_items(self._nodes[item_topic_name])
                            del self._nodes[item_topic_name]
                return
        # simple type or simple type array
        if topic_name in self._nodes and self._nodes[topic_name] is not None:
            node = self._nodes[topic_name]
            dv = ua.Variant(msg, node.get_data_type_as_variant_type())
            node.set_value(dv)


    def recursive_delete_items(self, item):
        self._publisher.unregister()
        self._subscriber.unregister()
        for child in item.get_children():
            self.recursive_delete_items(child)
            if child in self._nodes:
                del self._nodes[child]
            self.server.server.delete_nodes([child])
        self.server.server.delete_nodes([item])
        if len(self.parent.get_children()) == 0:
            self.server.server.delete_nodes([self.parent])


    def create_message_instance(self, node):
        for child in node.get_children():
            name = child.get_display_name().Text
            if hasattr(self.message_instance, name):
                if child.get_node_class() == ua.NodeClass.Variable:
                    setattr(self.message_instance, name,
                            correct_type(child, type(getattr(self.message_instance, name))))
                elif child.get_node_class == ua.NodeClass.Object:
                    setattr(self.message_instance, name, self.create_message_instance(child))
        return self.message_instance  # Converts the value of the node to that specified in the ros message we are trying to fill. Casts python ints


    def recursive_create_objects(self, topic_name, idx, parent):
        hierachy = topic_name.split('/')
        if len(hierachy) == 0 or len(hierachy) == 1:
            return parent
        for name in hierachy:
            if name != '':
                try:
                    nodewithsamename = self.server.find_topics_node_with_same_name(name, idx)
                    if nodewithsamename is not None:
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, nodewithsamename)
                    else:
                        # if for some reason 2 services with exactly same name are created use hack>: add random int, prob to hit two
                        # same ints 1/10000, should be sufficient
                        newparent = parent.add_object(
                            ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                            ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, newparent)
                # thrown when node with parent name is not existent in server
                except IndexError, common.UaError:
                    newparent = parent.add_object(
                        ua.NodeId(name + str(random.randint(0, 10000)), parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                        ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                    return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, newparent)

        return parent


# to unsigned integers as to fulfill ros specification. Currently only uses a few different types,
# no other types encountered so far.
def correct_type(node, typemessage):
    data_value = node.get_data_value()
    result = node.get_value()
    if isinstance(data_value, ua.DataValue):
        if typemessage.__name__ == "float":
            result = numpy.float(result)
        if typemessage.__name__ == "double":
            result = numpy.double(result)
        if typemessage.__name__ == "int":
            result = int(result) & 0xff
    else:
        rospy.logerr("can't convert: " + str(node.get_data_value.Value))
        return None
    return result


def extract_array_info(type_str):
    array_size = None
    if '[' in type_str and type_str[-1] == ']':
        type_str, array_size_str = type_str.split('[', 1)
        array_size_str = array_size_str[:-1]
        if len(array_size_str) > 0:
            array_size = int(array_size_str)
        else:
            array_size = 0

    return type_str, array_size


def create_topic_variable(parent, idx, topic_name, topic_text, type_name):
    rospy.logdebug("Creating topic variable: '%s' with type_name: '%s'", topic_name, type_name)

    base_type_str, array_size = extract_array_info(type_name)

    if base_type_str == 'bool':
        dv = ua.Variant(False, ua.VariantType.Boolean)  if array_size is None else ua.Variant([], ua.VariantType.Boolean)
        dt = ua.NodeId(ua.ObjectIds.Boolean, 0)
    elif base_type_str == 'int8':
        dv = ua.Variant(0, ua.VariantType.SByte)        if array_size is None else ua.Variant([], ua.VariantType.SByte)
        dt = ua.NodeId(ua.ObjectIds.SByte, 0)
    elif base_type_str == 'byte' or base_type_str == 'uint8':
        dv = ua.Variant(0, ua.VariantType.Byte)         if array_size is None else ua.Variant([], ua.VariantType.Byte)
        dt = ua.NodeId(ua.ObjectIds.Byte, 0)
    elif base_type_str == 'int16':
        dv = ua.Variant(0, ua.VariantType.Int16)        if array_size is None else ua.Variant([], ua.VariantType.Int16)
        dt = ua.NodeId(ua.ObjectIds.Int16, 0)
    elif base_type_str == 'uint16':
        dv = ua.Variant(0, ua.VariantType.UInt16)       if array_size is None else ua.Variant([], ua.VariantType.UInt16)
        dt = ua.NodeId(ua.ObjectIds.UInt16, 0)
    elif base_type_str == 'int' or base_type_str == 'int32':
        dv = ua.Variant(0, ua.VariantType.Int32)        if array_size is None else ua.Variant([], ua.VariantType.Int32)
        dt = ua.NodeId(ua.ObjectIds.Int32, 0)
    elif base_type_str == 'uint32':
        dv = ua.Variant(0, ua.VariantType.UInt32)       if array_size is None else ua.Variant([], ua.VariantType.UInt32)
        dt = ua.NodeId(ua.ObjectIds.UInt32, 0)
    elif base_type_str == 'int64':
        dv = ua.Variant(0, ua.VariantType.Int64)        if array_size is None else ua.Variant([], ua.VariantType.Int64)
        dt = ua.NodeId(ua.ObjectIds.Int64, 0)
    elif base_type_str == 'uint64':
        dv = ua.Variant(0, ua.VariantType.UInt64)       if array_size is None else ua.Variant([], ua.VariantType.UInt64)
        dt = ua.NodeId(ua.ObjectIds.UInt64, 0)
    elif base_type_str == 'float' or base_type_str == 'float32' or base_type_str == 'float64':
        dv = ua.Variant(0.0, ua.VariantType.Float)      if array_size is None else ua.Variant([], ua.VariantType.Float)
        dt = ua.NodeId(ua.ObjectIds.Float, 0)
    elif base_type_str == 'double':
        dv = ua.Variant(0.0, ua.VariantType.Double)     if array_size is None else ua.Variant([], ua.VariantType.Double)
        dt = ua.NodeId(ua.ObjectIds.Double, 0)
    elif base_type_str == 'string':
        dv = ua.Variant('', ua.VariantType.String)      if array_size is None else ua.Variant([], ua.VariantType.String)
        dt = ua.NodeId(ua.ObjectIds.String, 0)
    else:
        rospy.logerr("can't create node with type %s", str(base_type_str))
        return None

    node = parent.add_variable(ua.NodeId(topic_name, parent.nodeid.NamespaceIndex),
                               ua.QualifiedName(topic_text, parent.nodeid.NamespaceIndex),
                               val=dv, datatype=dt)
    return node

# Used to delete obsolete topics
def number_of_subscribers(nametolookfor, topicsDict):
    # rosout only has one subscriber/publisher at all times, so ignore.
    if nametolookfor != "/rosout":
        ret = topicsDict[nametolookfor]._subscriber.get_num_connections()
    else:
        ret = 2
    return ret


# use to not get dict changed during iteration errors
def refresh_dict(ros_namespace, topics_dict, server, idx_topics):
    # get current published topics
    topics = rospy.get_published_topics(ros_namespace)

    to_be_deleted = []
    for opcua_topic_name in topics_dict:
        found = False
        for topic_name, topic_type in topics:
            if opcua_topic_name == topic_name:
                found = True
        if not found:
            topics_dict[opcua_topic_name].recursive_delete_items(server.get_node(ua.NodeId(opcua_topic_name, idx_topics)))
            to_be_deleted.append(opcua_topic_name)
    for name in to_be_deleted:
        del topics_dict[name]


def refresh_topics(ros_namespace, server, topics_dict, idx_topics, topics):
    ros_topics = rospy.get_published_topics(ros_namespace)

    rospy.logdebug(str(ros_topics))
    rospy.logdebug(str(rospy.get_published_topics('/move_base_simple')))

    for topic_name, topic_type in ros_topics:

        if topic_name not in topics_dict or topics_dict[topic_name] is None:
            splits = topic_name.split('/')

            if splits[-1] not in ["status", "cancel", "goal", "feedback", "result"]:
                # rospy.loginfo("Ignoring normal topics for debugging...")
                topic = OpcUaROSTopic(server, topics, idx_topics, topic_name, topic_type)
                topics_dict[topic_name] = topic
            else:
                rospy.logdebug("Found an action: %s", str(topic_name))
                continue
                # correct_name = ros_actions.get_correct_name(topic_name)
                # if correct_name not in actionsdict:
                #     try:
                #         rospy.loginfo("Creating Action with name: " + correct_name)
                #         actionsdict[correct_name] = ros_actions.OpcUaROSAction(server,
                #                                                                actions,
                #                                                                idx_actions,
                #                                                                correct_name,
                #                                                                get_goal_type(correct_name),
                #                                                                get_feedback_type(correct_name))
                #     except (ValueError, TypeError, AttributeError) as e:
                #         print(e)
                #         rospy.logerr("Error while creating Action Objects for Action " + topic_name)

        elif number_of_subscribers(topic_name, topics_dict) <= 1 and "rosout" not in topic_name:
            topics_dict[topic_name].recursive_delete_items(server.server.get_node(ua.NodeId(topic_name, idx_topics)))
            del topics_dict[topic_name]
            ros_server.own_rosnode_cleanup()

    ros_topics.refresh_dict(ros_namespace, topics_dict, server, idx_topics)


def get_feedback_type(action_name):
    try:
        type, name, fn = rostopic.get_topic_type(action_name + "/feedback")
        return type
    except rospy.ROSException as e:
        try:
            type, name, fn = rostopic.get_topic_type(action_name + "/Feedback", e)
            return type
        except rospy.ROSException as e2:
            rospy.logerr("Couldn't find feedback type for action " + action_name, e2)
            return None


def get_goal_type(action_name):
    try:
        type, name, fn = rostopic.get_topic_type(action_name + "/goal")
        return type
    except rospy.ROSException as e:
        try:
            type, name, fn = rostopic.get_topic_type(action_name + "/Goal", e)
            return type
        except rospy.ROSException as e2:
            rospy.logerr("Couldn't find goal type for action " + action_name, e2)
            return None
