#!/usr/bin/python
import sys
import time
import logging

import rospy
import rosgraph
import rosnode
import std_srvs.srv

import opcua
from opcua import ua

import ros_services
import ros_topics
import ros_actions
import ros_utils


# Returns the hierachy as one string from the first remaining part on.
def nextname(hierachy, index_of_last_processed):
    try:
        output = ""
        counter = index_of_last_processed + 1
        while counter < len(hierachy):
            output += hierachy[counter]
            counter += 1
        return output
    except Exception as e:
        rospy.logerr("Error encountered ", e)


def own_rosnode_cleanup():
    pinged, unpinged = rosnode.rosnode_ping_all()
    if unpinged:
        master = rosgraph.Master(rosnode.ID)
        # noinspection PyTypeChecker
        rosnode.cleanup_master_blacklist(master, unpinged)


class ROSServer:

    def __init__(self, endpoint, server_name):

        self.endpoint = endpoint
        self.server_name = server_name

        self.ros_namespace = rospy.get_param("~ros_namespace", '/')
        self.topics_dict = {}
        self.services_dict = {}
        self.actions_dict = {}

        #
        self.filter_services = rospy.get_param("~filter/services")
        self.filter_topics = rospy.get_param("~filter/topics")


        self.server = opcua.Server()
        self.server.set_endpoint(endpoint)
        self.server.set_server_name(server_name)


    def server_config(self, server):
        """
        ServerProfileArray lists the Profiles that the Server supports. See OPC 10000-7
        for the definitions of Server Profiles. This list should be limited to the Profiles
        the Server supports in its current configuration.
        """
        #node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        #node.set_value(ua.Variant(0, ua.VariantType))

        """
        LocaleIdArray is an array of LocaleIds that are known to be supported by the Server.
        The Server might not be aware of all LocaleIds that it supports because it may provide
        access to underlying servers, systems or devices that do not report the LocaleIds that
        they support.
        """
        #node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        #node.set_value(ua.Variant(0, ua.VariantType))

        """
        MinSupportedSampleRate defines the minimum supported sample rate, including 0,
        which is supported by the Server.
        """
        #node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        #node.set_value(ua.Variant(0, ua.VariantType))

        """
        MaxBrowseContinuationPoints is an integer specifying the maximum number of parallel
        continuation points of the Browse Service that the Server can support per session.
        The value specifies the maximum the Server can support under normal circumstances,
        so there is no guarantee the Server can always support the maximum.
        The client should not open more Browse calls with open continuation points than exposed
        in this Variable. The value 0 indicates that the Server does not restrict the number of
        parallel continuation points the client should use.
        """
        node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        node.set_value(ua.Variant(0, ua.VariantType.UInt16))

        """
        MaxQueryContinuationPoints is an integer specifying the maximum number of parallel
        continuation points of the QueryFirst Services that the Server can support per session.
        The value specifies the maximum the Server can support under normal circumstances, so
        there is no guarantee the Server can always support the maximum.
        The client should not open more QueryFirst calls with open continuation points than
        exposed in this Variable. The value 0 indicates that the Server does not restrict the
        number of parallel continuation points the client should use.
        """
        node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxQueryContinuationPoints, 0))
        node.set_value(ua.Variant(0, ua.VariantType.UInt16))

        """
        SoftwareCertificates is an array of SignedSoftwareCertificates containing all
        SoftwareCertificates supported by the Server.
        A SoftwareCertificate identifies capabilities of the Server. It contains the
        list of Profiles supported by the Server. Profiles are described in OPC 10000-7.
        """
        #node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        #node.set_value(ua.Variant(0, ua.VariantType))


    def start(self):
        self.server.start()
        rospy.loginfo("Started OPC-UA Server %s/%s", self.endpoint, self.server_name)

        self.server_config(self.server)

        # setup our own namespaces, this is expected
        # two different namespaces to make getting the correct node easier for get_node (otherwise had object for service and topic with same name
        uri_topics = "http://ros.org/topics"
        uri_services = "http://ros.org/services"
        uri_actions = "http://ros.org/actions"
        self.idx_topics = self.server.register_namespace(uri_topics)
        self.idx_services = self.server.register_namespace(uri_services)
        self.idx_actions = self.server.register_namespace(uri_actions)

        # get Objects node, this is where we should put our custom stuff
        objects = self.server.get_objects_node()
        # one object per type we are watching
        self.topics_object = objects.add_folder(self.idx_topics, "ROS-Topics")
        self.services_object = objects.add_folder(self.idx_services, "ROS-Services")
        self.actions_object = objects.add_folder(self.idx_actions, "ROS-Actions")


    def stop(self):
        self.server.stop()
        rospy.loginfo("Stopped OPC-UA Server %s/%s", self.endpoint, self.server_name)


    def refresh(self, clean_all=False):
        rospy.loginfo("Refreshing OPC-UA Server %s/%s ...", self.endpoint, self.server_name)

        #ros_services.clean_dict(self.ros_namespace, self, self.services_dict, self.idx_services, clean_all)
        ros_topics.clean_dict(self.ros_namespace, self, self.topics_dict, self.idx_topics, clean_all)

        #ros_services.refresh_services(self.ros_namespace, self, self.services_dict, self.idx_services, self.services_object)
        ros_topics.refresh_topics(self.ros_namespace, self, self.topics_dict, self.idx_topics, self.topics_object)
        # ros_actions.refresh_actions(ros_server.ros_namespace, ros_server, ros_server.actions_dict, ros_server.idx_actions, ros_server.actions_object)

        return True


    def find_service_node_with_same_name(self, name, idx):
        rospy.logdebug("Reached ServiceCheck for name " + name)
        for service in self.services_dict:
            rospy.logdebug("Found name: " + str(self.services_dict[service].parent.nodeid.Identifier))
            if self.services_dict[service].parent.nodeid.Identifier == name:
                rospy.logdebug("Found match for name: " + name)
                return self.services_dict[service].parent
        return None


    def find_topics_node_with_same_name(self, name, idx):
        rospy.logdebug("Reached TopicCheck for name " + name)
        for topic in self.topics_dict:
            rospy.logdebug("Found name: " + str(self.topics_dict[topic].parent.nodeid.Identifier))
            if self.topics_dict[topic].parent.nodeid.Identifier == name:
                rospy.logdebug("Found match for name: " + name)
                return self.topics_dict[topic].parent
        return None


    def find_action_node_with_same_name(self, name, idx):
        rospy.logdebug("Reached ActionCheck for name " + name)
        for topic in self.actions_dict:
            rospy.logdebug("Found name: " + str(self.actions_dict[topic].parent.nodeid.Identifier))
            if self.actions_dict[topic].parent.nodeid.Identifier == name:
                rospy.logdebug("Found match for name: " + name)
                return self.actions_dict[topic].parent
        return None



def refresh(req):
    res = std_srvs.srv.TriggerResponse()

    if ros_server.refresh(clean_all=True):
        res.success = True
        res.message = ""
    else:
        res.success = False
        res.message = ""

    return res


if __name__ == '__main__':

    # Node
    rospy.init_node("rosopcua", log_level=rospy.INFO)

    # Parameters
    server_endpoint = rospy.get_param("~server/endpoint")
    server_name = rospy.get_param("~server/name")

    startup_time = rospy.get_param("~startup_time", 0.0)
    refresh_time = rospy.get_param("~refresh_time", 10.0)

    # Services
    refresh_srv = rospy.Service("~refresh", std_srvs.srv.Trigger, refresh)

    # wait that all nodes started up
    rospy.sleep(startup_time)

    # ROS OPC-UA Server
    ros_server = ROSServer(server_endpoint, server_name)
    ros_server.start()

    # Loop
    rate = rospy.Rate(1.0/refresh_time)
    while not rospy.is_shutdown():
        # ros_topics starts a lot of publisher/subscribers, might slow everything down quite a bit.
        ros_services.refresh_services(ros_server.ros_namespace, ros_server, ros_server.services_dict, ros_server.idx_services, ros_server.services_object)
        ros_topics.refresh_topics(ros_server.ros_namespace, ros_server, ros_server.topics_dict, ros_server.idx_topics, ros_server.topics_object)
        # ros_actions.refresh_actions(ros_server.ros_namespace, ros_server, ros_server.actions_dict, ros_server.idx_actions, ros_server.actions_object)

        rate.sleep()

    ros_server.stop()
