#!/usr/bin/env python

# Thanks to:
# https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_topic/src/rqt_topic/topic_widget.py
import random
import numpy

import rospy
import rostopic
import roslib
import roslib.message
from opcua import ua, uamethod

import ros_server
import ros_actions
import ros_utils



# use to not get dict changed during iteration errors
def refresh_dict(ros_namespace, ros_server, topics_dict, idx_topics):
    # get current published topics
    ros_topics = rospy.get_published_topics(ros_namespace)

    to_be_deleted = []
    for node_name in topics_dict:
        # search if node_name is in ros_topics
        found = False
        for topic_name, topic_type in ros_topics:
            if node_name == topic_name:
                found = True
        # if not found delete it
        if not found:
            topics_dict[node_name].recursive_delete_node(ros_server.server.get_node(ua.NodeId(node_name, idx_topics)))
            to_be_deleted.append(node_name)

    for topic_name in to_be_deleted:
        del topics_dict[topic_name]


def refresh_topics(ros_namespace, ros_server, topics_dict, idx_topics, topics_object):
    ros_topics = rospy.get_published_topics(ros_namespace)

    # rospy.logdebug(str(topics))
    # rospy.logdebug(str(rospy.get_published_topics('/move_base_simple')))

    for topic_name, topic_type in ros_topics:

        if topic_name not in topics_dict or topics_dict[topic_name] is None:
            # splits = topic_name.split('/')

            # if splits[-1] not in ["status", "cancel", "goal", "feedback", "result"]:
                # rospy.loginfo("Ignoring normal topics for debugging...")
            opcua_topic = OpcUaROSTopic(ros_server, topics_object, idx_topics, topic_name, topic_type)
            topics_dict[topic_name] = opcua_topic

        elif number_of_subscribers(topic_name, topics_dict) <= 1 and "rosout" not in node_name:
            topics_dict[topic_name].recursive_delete_node(ros_server.server.get_node(ua.NodeId(topic_name, idx_topics)))
            del topics_dict[topic_name]
            ros_server.own_rosnode_cleanup()

    refresh_dict(ros_namespace, ros_server, topics_dict, idx_topics)


# Used to delete obsolete topics
def number_of_subscribers(node_name, topics_dict):
    # rosout only has one subscriber/publisher at all times, so ignore.
    if node_name != "/rosout":
        ret = topics_dict[node_name]._subscriber.get_num_connections()
    else:
        ret = 2
    return ret


class OpcUaROSTopic:

    def __init__(self, server, parent, idx, topic_name, topic_type):
        self.server = server
        self.parent = self.recursive_create_objects(topic_name, idx, parent)
        self.idx = idx
        self.nodes = {}

        self.topic_name = topic_name
        self.topic_type = topic_type

        try:
            self.msg_class = roslib.message.get_message_class(topic_type)
            self.msg_instance = self.msg_class()
        except rospy.ROSException:
            rospy.logfatal("Couldn't find message class for type '%s'", topic_type)
            return

        self.recursive_create_node(self.parent, idx, self.topic_name, self.topic_type, self.msg_instance, True)

        self._subscriber = rospy.Subscriber(self.topic_name, roslib.message.get_message_class(topic_type), self.message_callback)
        self._publisher = rospy.Publisher(self.topic_name, roslib.message.get_message_class(topic_type), queue_size=1)
        rospy.loginfo("Created ROS Topic with name: '%s'", str(self.topic_name))


    def recursive_create_node(self, parent, idx, name, type_name, msg, top_level=False):
        qname = name.split('/')[-1]
        # if '[' in topic_text:
        #     topic_text = topic_text[topic_text.index('['):]

        if hasattr(msg, '__slots__') and hasattr(msg, '_slot_types'):
            # compley type
            child = parent.add_object(ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                                      ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
            #
            child.add_property(ua.NodeId(name + ".Type", idx),
                               ua.QualifiedName("Type", parent.nodeid.NamespaceIndex), type_name)
            #
            if top_level:
                child.add_method(ua.NodeId(name + ".Update", parent.nodeid.NamespaceIndex),
                                 ua.QualifiedName("Update", parent.nodeid.NamespaceIndex),
                                 self.opcua_update_callback, [], [])
            #
            for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
                self.recursive_create_node(child, idx, name + '/' + slot_name, slot_type, getattr(msg, slot_name))
            #
            self.nodes[name] = child

        else:

            base_type_str, array_size = ros_utils.extract_array_info(type_name)

            try:
                base_class = roslib.message.get_message_class(base_type_str)
                base_instance = base_class()
            except (ValueError, TypeError):
                base_instance = None

            if array_size is not None and hasattr(base_instance, '__slots__'):
                for index in range(array_size):
                    self.recursive_create_node(parent, idx, name + '[%d]' % index, base_type_str, base_instance)
            else:
                node = create_node_variable(parent, name, qname, type_name)
                node.set_writable(True)
                self.nodes[name] = node

        return


    def recursive_delete_node(self, node):
        # Unsubscribe OPC-UA node from ros topic
        self._publisher.unregister()
        self._subscriber.unregister()

        # delete children
        for child in node.get_children():
            self.recursive_delete_node(child)
            if child in self.nodes:
                del self.nodes[child]
            self.server.server.delete_nodes([child])

        # delete the node
        self.server.server.delete_nodes([node])

        # if parent have no children delete it
        if len(self.parent.get_children()) == 0:
            self.server.server.delete_nodes([self.parent])


    def message_callback(self, msg):
        self.update_value(self.topic_name, msg)


    @uamethod
    def opcua_update_callback(self, parent):
        try:
            for node_key in self.nodes.keys():
                child = self.nodes[node_key]
                name = child.get_display_name().Text
                if hasattr(self.msg_instance, name):
                    if child.get_node_class() == ua.NodeClass.Variable:
                        print child.get_value()
                        setattr(self.msg_instance, name, child.get_value())
                        # correct_type(child, type(getattr(self.msg_instance, name))))
                    elif child.get_node_class() == ua.NodeClass.Object:
                        setattr(self.msg_instance, name, self.create_msg_instance(child))
            # publish msg
            self._publisher.publish(self.msg_instance)
        except rospy.ROSException as ex:
            rospy.logerr("Error while updating OPC-UA node: '%s'", self.topic_name, ex)
            self.server.server.delete_nodes([self.parent])


    def update_value(self, node_name, msg):

        if hasattr(msg, '__slots__') and hasattr(msg, '_slot_types'):
            # complex type
            for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
                self.update_value(node_name + '/' + slot_name, getattr(msg, slot_name))
            return

        if type(msg) in (list, tuple):

            if len(msg) > 0 and hasattr(msg[0], '__slots__'):
                # complex type array
                for index, slot in enumerate(msg):
                    if node_name + '[%d]' % index in self.nodes:
                        self.update_value(node_name + '[%d]' % index, slot)
                    else:
                        if node_name in self.nodes:
                            base_type_str, array_size = ros_utils.extract_array_info(self.nodes[node_name].text(self.type_name))
                            self.recursive_create_node(self.nodes[node_name], node_name + '[%d]' % index, base_type_str, slot, None)

                # remove obsolete children
                if node_name in self.nodes:

                    if len(msg) < len(self.nodes[node_name].get_children()):

                        for i in range(len(msg), len(self.nodes[node_name].get_children())):
                            self.recursive_delete_node(self.nodes[node_name + '[%d]' % i])
                            del self.nodes[node_name + '[%d]' % i]
                return

        # simple type or simple type array
        if node_name in self.nodes and self.nodes[node_name] is not None:
            node = self.nodes[node_name]
            dv = ua.Variant(msg, node.get_data_type_as_variant_type())
            node.set_value(dv)


    def create_msg_instance(self, node):
        for child in node.get_children():
            name = child.get_display_name().Text
            if hasattr(self.msg_instance, name):
                if child.get_node_class() == ua.NodeClass.Variable:
                    setattr(self.msg_instance, name,
                            correct_type(child, type(getattr(self.msg_instance, name))))
                elif child.get_node_class == ua.NodeClass.Object:
                    setattr(self.msg_instance, name, self.create_msg_instance(child))
        return self.msg_instance  # Converts the value of the node to that specified in the ros message we are trying to fill. Casts python ints


    def recursive_create_objects(self, topic_name, idx, parent):
        hierachy = topic_name.split('/')
        if len(hierachy) == 0 or len(hierachy) == 1:
            return parent
        for name in hierachy:
            if name != '':
                try:
                    node_with_same_name = self.server.find_topics_node_with_same_name(name, idx)
                    if node_with_same_name is not None:
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, node_with_same_name)
                    else:
                        # if for some reason 2 services with exactly same name are created use hack>: add random int, prob to hit two
                        # same ints 1/10000, should be sufficient
                        new_parent = parent.add_object(
                            ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                            ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, new_parent)
                # thrown when node with parent name is not existent in server
                except IndexError, common.UaError:
                    new_parent = parent.add_object(
                        ua.NodeId(name + str(random.randint(0, 10000)), parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                        ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                    return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, new_parent)

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


def create_node_variable(parent, name, qname, type_name):
    rospy.logdebug("Creating node variable: '%s' with type_name: '%s'", name, type_name)

    base_type_str, array_size = ros_utils.extract_array_info(type_name)

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

    node = parent.add_variable(ua.NodeId(name, parent.nodeid.NamespaceIndex),
                               ua.QualifiedName(qname, parent.nodeid.NamespaceIndex),
                               val=dv, datatype=dt)
    return node
