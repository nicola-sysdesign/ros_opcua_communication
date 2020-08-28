# Thanks to:
# https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_service_caller/src/rqt_service_caller/service_caller_widget.py
import math
import random
import time

import rospy
import genpy
import roslib
import rosservice
from opcua import ua, uamethod, common

import ros_server
import ros_utils


def clean_dict(ros_namespace, ros_server, services_dict, idx, clean_all=False):
    ros_services = rosservice.get_service_list()

    to_be_deleted = []
    for node_name in services_dict:

        if (node_name not in ros_services) or (clean_all == True):

            services_dict[node_name].recursive_delete_node(ros_server.server.get_node(ua.NodeId(node_name, idx)))

            if len(services_dict[node_name].parent.get_children()) == 0:
                ros_server.server.delete_nodes([services_dict[node_name].parent])

            to_be_deleted.append(node_name)

    for node_name in to_be_deleted:
        del services_dict[node_name]


    # for node_name in services_dict:
    #     #
    #     found = False
    #     for service_name in ros_services:
    #         if node_name == service_name:
    #             found = True
    #     #
    #     if not found and services_dict[node_name] is not None:
    #         services_dict[node_name].recursive_delete_nodes(ros_server.server.get_node(ua.NodeId(node_name, idx)))
    #         to_be_deleted.append(node_name)
    #     #
    #     if len(services_dict[node_name].parent.get_children()) == 0:
    #         ros_server.server.delete_nodes([services_dict[node_name].parent])
    #
    # for node_name in to_be_deleted:
    #     del services_dict[node_name]


def refresh_services(ros_namespace, ros_server, services_dict, idx, services_object):
    ros_services = rosservice.get_service_list(namespace=ros_namespace)

    for service_name in ros_services:

        if service_name not in ros_server.filter_services:
            continue

        try:
            if service_name not in services_dict or services_dict[service_name] is None:
                opcua_service = OpcUaROSService(ros_server, services_object, idx, service_name, rosservice.get_service_type(service_name))
                services_dict[service_name] = opcua_service
        except (rosservice.ROSServiceException, rosservice.ROSServiceIOException) as ex:
            rospy.logerr("Error when trying to refresh services", ex)
        except TypeError as ex:
            rospy.logerr("Error when logging an Exception, can't convert everything to string")

    clean_dict(ros_namespace, ros_server, services_dict, idx)


class OpcUaROSService:

    def __init__(self, ros_server, parent, idx, service_name, service_type):
        self.server = ros_server
        self.parent = parent # self.recursive_create_objects(service_name, idx, parent)
        self.idx = idx

        self.service_name = service_name
        self.service_type = service_type
        rospy.logdebug("service_name: '%s'", self.service_name)
        rospy.logdebug("service_type: '%s'", self.service_type)

        self.counter = 0
        self._nodes = {}
        self.expressions = {}
        self._eval_locals = {}

        try:
            self.srv_class = rosservice.get_service_class_by_name(self.service_name)
            self.srv_instance = self.srv_class()
        except rospy.ROSException:
            rospy.logfatal("Couldn't find service class for type '%s'", self.service_type)
            return

        self.proxy = rospy.ServiceProxy(self.service_name, rosservice.get_service_class_by_name(self.service_name))


        # for module in (math, random, time):
        #     self._eval_locals.update(module.__dict__)
        #
        # self._eval_locals['genpy'] = genpy
        # del self._eval_locals['__name__']
        # del self._eval_locals['__doc__']

        # Build the Array of inputs
        self.req_class = self.srv_class._request_class()
        self.res_class = self.srv_class._response_class()
        self.inputs  = ros_utils.ros_msg_to_arguments(self.srv_class._request_class())
        self.outputs = ros_utils.ros_msg_to_arguments(self.srv_class._response_class())

        name = self.service_name
        qname = self.service_name.split('/')[-1]
        self.method = self.parent.add_method(ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                                             ua.QualifiedName(name, parent.nodeid.NamespaceIndex),
                                             self.call_service, self.inputs, self.outputs)

        rospy.loginfo("Created ROS Service: %s", self.service_name)


    def recursive_create_objects(self, name, idx, parent):
        hierachy = name.split('/')

        if len(hierachy) == 0 or len(hierachy) == 1:
            return parent

        for name in hierachy:
            if name != '':
                try:
                    node_with_same_name = self.server.find_service_node_with_same_name(name, idx)
                    rospy.logdebug("node_with_same_name for name: " + str(name) + " is : " + str(node_with_same_name))
                    if node_with_same_name is not None:
                        rospy.logdebug("recursive call for same name for: " + name)
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, node_with_same_name)
                    else:
                        new_parent = parent.add_object(ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                                                       ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, new_parent)
                except IndexError, common.uaerrors.UaError:
                    new_parent = parent.add_object(ua.NodeId(name + str(random.randint(0, 10000)), parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                                                   ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                    return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, new_parent)
        return parent


    @uamethod
    def call_service(self, parent, *input_args):
        rospy.loginfo("Call Service: %s", self.service_name)
        rospy.logdebug("input_args: %s", str(input_args))

        req, input_idx = self.create_service_request(self.srv_class._request_class(), input_args)
        rospy.logdebug("Request:\n%s", str(req))

        try:
            res = self.proxy.call(req)
            rospy.logdebug("Response:\n%s", str(res))
        except (TypeError, rosservice.ROSServiceException, rospy.ROSInterruptException, rospy.ROSSerializationException) as ex:
            rospy.logerr("Error when calling service: %s", self.service_name, ex)
            return

        output_args = ros_utils.ros_msg_to_variants(res)
        rospy.logdebug("output_args: %s", str(output_args))
        return output_args


    def create_service_request(self, req, input_args, input_idx=0):
        for slot_name, slot_type in zip(req.__slots__, req._slot_types):
            slot_value = getattr(req, slot_name)

            if hasattr(slot_value, '_type'):
                slot_value, input_idx = self.create_service_request(slot_value, input_args, input_idx)
                setattr(req, slot_name, slot_value)
            else:
                arg = input_args[input_idx]
                setattr(req, slot_name, arg)
                input_idx += 1

        return req, input_idx


    def recursive_delete_node(self, node):
        # close ros proxy service
        self.proxy.close()

        for child in node.get_children():
            self.recursive_delete_node(child)

            if child in self._nodes:
                del self._nodes[child]

            self.server.server.delete_nodes([child])

        self.server.server.delete_nodes([self.method])
        ros_server.own_rosnode_cleanup()
