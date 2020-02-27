# Thanks to:
# https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_topic/src/rqt_topic/topic_widget.py
import random
import numpy

import rospy
import roslib
import roslib.message
import rostopic
from opcua import ua, uamethod

import ros_server
import ros_actions
import ros_utils


# use to not get dict changed during iteration errors
def refresh_dict(ros_namespace, ros_server, topics_dict, idx):
    ros_topics = rospy.get_published_topics(namespace=ros_namespace)
    topic_names = zip(*ros_topics)[0]
    topic_types = zip(*ros_topics)[1]

    to_be_deleted = []
    for node_name in topics_dict:

        if node_name not in topic_names:

            topics_dict[node_name].recursive_delete_node(ros_server.server.get_node(ua.NodeId(node_name, idx)))

            to_be_deleted.append(node_name)

    for node_name in to_be_deleted:
        del topics_dict[node_name]


def refresh_topics(ros_namespace, ros_server, topics_dict, idx, topics_object):
    ros_topics = rospy.get_published_topics(namespace=ros_namespace)

    #
    for topic_name, topic_type in ros_topics:

        if topic_name not in ros_server.filter_topics:
            continue

        if topic_name not in topics_dict or topics_dict[topic_name] is None:
            # splits = topic_name.split('/')
            # if splits[-1] not in ["status", "cancel", "goal", "feedback", "result"]:
                # rospy.loginfo("Ignoring normal topics for debugging...")
            opcua_topic = OpcUaROSTopic(ros_server, topics_object, idx, topic_name, topic_type)
            topics_dict[topic_name] = opcua_topic

        # elif number_of_subscribers(topic_name, topics_dict) <= 1 and "rosout" not in node_name:
        #     topics_dict[topic_name].recursive_delete_node(ros_server.server.get_node(ua.NodeId(topic_name, idx_topics)))
        #     del topics_dict[topic_name]
        #     ros_server.own_rosnode_cleanup()

    refresh_dict(ros_namespace, ros_server, topics_dict, idx)


# Used to delete obsolete topics
def number_of_subscribers(node_name, topics_dict):
    # rosout only has one subscriber/publisher at all times, so ignore.
    if node_name != "/rosout":
        ret = topics_dict[node_name].subscriber.get_num_connections()
    else:
        ret = 2
    return ret


class OpcUaROSTopic:

    def __init__(self, ros_server, parent, idx, topic_name, topic_type):
        self.server = ros_server
        self.parent = parent # self.recursive_create_objects(parent, idx, topic_name)
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

        self.subscriber = rospy.Subscriber(self.topic_name, roslib.message.get_message_class(topic_type), self.message_callback)
        self.publisher  = rospy.Publisher(self.topic_name, roslib.message.get_message_class(topic_type), queue_size=1)

        rospy.loginfo("Created ROS Topic: %s", self.topic_name)


    def recursive_create_objects(self, parent, idx, topic_name):
        hierachy = topic_name.split('/')
        if len(hierachy) == 0 or len(hierachy) == 1:
            return parent

        for name in hierachy:
            if name != '':
                try:
                    node_with_same_name = self.server.find_topics_node_with_same_name(name, idx)
                    if node_with_same_name is not None:
                        return self.recursive_create_objects(node_with_same_name, idx, ros_server.nextname(hierachy, hierachy.index(name)))
                    else:
                        child = parent.add_object(
                            ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                            ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                        return self.recursive_create_objects(child, idx, ros_server.nextname(hierachy, hierachy.index(name)))
                except IndexError, common.UaError:
                    # if for some reason 2 services with exactly same name are created use hack>: add random int, prob to hit two
                    # same ints 1/10000, should be sufficient
                    child = parent.add_object(
                        ua.NodeId(name + str(random.randint(0, 10000)), parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                        ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                    return self.recursive_create_objects(child, idx, ros_server.nextname(hierachy, hierachy.index(name)))

        return parent


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
        self.publisher.unregister()
        self.subscriber.unregister()

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
        self.update_node_value(self.topic_name, msg)


    def update_node_value(self, node_name, msg):

        if hasattr(msg, '__slots__') and hasattr(msg, '_slot_types'):
            # complex type
            for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
                slot_value = getattr(msg, slot_name)
                self.update_node_value(node_name + '/' + slot_name, slot_value)
            return

        if type(msg) in (list, tuple):

            if len(msg) > 0 and hasattr(msg[0], '__slots__'):
                # complex type array
                for index, slot in enumerate(msg):
                    if node_name + '[%d]' % index in self.nodes:
                        self.update_node_value(node_name + '[%d]' % index, slot)
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


    @uamethod
    def opcua_update_callback(self, parent):

        msg = self.create_msg_instance(parent)

        try:
            # publish msg
            self.publisher.publish(msg)
        except rospy.ROSException as ex:
            rospy.logerr("Error while updating OPC-UA node: '%s'", self.topic_name, ex)
            self.server.server.delete_nodes([self.parent])


    # Converts the value of the node to that specified in the ros message we are
    # trying to fill. Casts python ints.
    def create_msg_instance(self, node):
        for child in node.get_children():
            display_name = child.get_display_name()
            slot_name = display_name.Text
            if hasattr(self.msg_instance, slot_name):
                if child.get_node_class() == ua.NodeClass.Variable:
                    slot_value = correct_type(child, type(getattr(self.msg_instance, name)))
                    setattr(self.msg_instance, slot_name, slot_value)
                    rospy.logdebug("updated slot '%s' with value: %s", slot_name, slot_value)
                elif child.get_node_class == ua.NodeClass.Object:
                    setattr(self.msg_instance, slot_name, self.create_msg_instance(child))

        return self.msg_instance


# to unsigned integers as to fulfill ros specification. Currently only uses a few different types,
# no other types encountered so far.
def correct_type(node, type_msg):
    data_value = node.get_data_value()
    result = node.get_value()
    if isinstance(data_value, ua.DataValue):
        if type_msg.__name__ == "float":
            result = numpy.float(result)
        if type_msg.__name__ == "double":
            result = numpy.double(result)
        if type_msg.__name__ == "int":
            result = int(result) & 0xff
    else:
        rospy.logerr("can't convert: " + str(node.get_data_value.Value))
        return None
    return result


def create_node_variable(parent, name, qname, type_name):
    rospy.logdebug("Creating node variable: '%s' of type: '%s'", name, type_name)

    base_type, array_size = ros_utils.extract_array_info(type_name)

    if base_type in ['bool']:
        if array_size is None:
            dv = ua.Variant(False, ua.VariantType.Boolean)
        else:
            dv = ua.Variant([], ua.VariantType.Boolean)
        dt = ua.NodeId(ua.ObjectIds.Boolean, 0)
    elif base_type in ['int8']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.SByte)
        else:
            dv = ua.Variant([], ua.VariantType.SByte)
        dt = ua.NodeId(ua.ObjectIds.SByte, 0)
    elif base_type in ['byte', 'uint8']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.Byte)
        else:
            dv = ua.Variant([], ua.VariantType.Byte)
        dt = ua.NodeId(ua.ObjectIds.Byte, 0)
    elif base_type in ['int16']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.Int16)
        else:
            dv = ua.Variant([], ua.VariantType.Int16)
        dt = ua.NodeId(ua.ObjectIds.Int16, 0)
    elif base_type in ['uint16']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.UInt16)
        else:
            dv = ua.Variant([], ua.VariantType.UInt16)
        dt = ua.NodeId(ua.ObjectIds.UInt16, 0)
    elif base_type in ['int', 'int32']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.Int32)
        else:
            dv = ua.Variant([], ua.VariantType.Int32)
        dt = ua.NodeId(ua.ObjectIds.Int32, 0)
    elif base_type in ['uint32']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.UInt32)
        else:
            dv = ua.Variant([], ua.VariantType.UInt32)
        dt = ua.NodeId(ua.ObjectIds.UInt32, 0)
    elif base_type in ['int64']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.Int64)
        else:
            dv = ua.Variant([], ua.VariantType.Int64)
        dt = ua.NodeId(ua.ObjectIds.Int64, 0)
    elif base_type in ['uint64']:
        if array_size is None:
            dv = ua.Variant(0, ua.VariantType.UInt64)
        else:
            dv = ua.Variant([], ua.VariantType.UInt64)
        dt = ua.NodeId(ua.ObjectIds.UInt64, 0)
    elif base_type in ['float', 'float32', 'float64']:
        if array_size is None:
            dv = ua.Variant(0.0, ua.VariantType.Float)
        else:
            dv = ua.Variant([], ua.VariantType.Float)
        dt = ua.NodeId(ua.ObjectIds.Float, 0)
    elif base_type in ['double']:
        if array_size is None:
            dv = ua.Variant(0.0, ua.VariantType.Double)
        else:
            dv = ua.Variant([], ua.VariantType.Double)
        dt = ua.NodeId(ua.ObjectIds.Double, 0)
    elif base_type in ['string']:
        if array_size is None:
            dv = ua.Variant('', ua.VariantType.String)
        else:
            dv = ua.Variant([], ua.VariantType.String)
        dt = ua.NodeId(ua.ObjectIds.String, 0)
    else:
        rospy.logerr("Can't create node variable of type '%s'", str(type_name))
        return None

    node = parent.add_variable(ua.NodeId(name, parent.nodeid.NamespaceIndex),
                               ua.QualifiedName(qname, parent.nodeid.NamespaceIndex),
                               val=dv, datatype=dt)
    return node
