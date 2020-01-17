#!/usr/bin/python
import sys
import time

import rospy
import rosgraph
import rosnode

import opcua
from opcua import Server, ua

import ros_services
import ros_topics


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

        self.ros_namespace = rospy.get_param("~ros_namespace")
        self.topics_dict = {}
        self.services_dict = {}
        self.actions_dict = {}

        self.server = opcua.Server()
        self.server.set_endpoint(endpoint)
        self.server.set_server_name(server_name)


    def start(self):
        self.server.start()
        rospy.loginfo("Started OPC-UA Server %s%s", self.endpoint, self.server_name)

        # fix B&R compatibility issue
        node = self.server.get_node(ua.NodeId(ua.ObjectIds.Server_ServerCapabilities_MaxBrowseContinuationPoints, 0))
        node.set_value(ua.Variant(0, ua.VariantType.UInt16))

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
        self.topics_object = objects.add_object(self.idx_topics, "ROS-Topics")
        self.services_object = objects.add_object(self.idx_services, "ROS-Services")
        self.actions_object = objects.add_object(self.idx_actions, "ROS-Actions")


    def stop(self):
        self.server.stop()
        rospy.loginfo("Stopped OPC-UA Server %s%s", self.endpoint, self.server_name)


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



if __name__ == '__main__':

    # Node
    rospy.init_node("rosopcua", log_level=rospy.DEBUG)

    # Parameters
    server_endpoint = rospy.get_param("~server/endpoint")
    server_name = rospy.get_param("~server/name")

    freq = rospy.get_param("~refresh_frequency")

    # ROS OPC-UA Server
    ros_server = ROSServer(server_endpoint, server_name)
    ros_server.start()

    # Loop
    rate = rospy.Rate(freq)
    while not rospy.is_shutdown():
        # ros_topics starts a lot of publisher/subscribers, might slow everything down quite a bit.
        ros_services.refresh_services(ros_server.ros_namespace, ros_server, ros_server.services_dict, ros_server.idx_services, ros_server.services_object)
        ros_topics.refresh_topics(ros_server.ros_namespace, ros_server, ros_server.topics_dict, ros_server.idx_topics, ros_server.topics_object)
        # ros_actions.refresh_actions(ros_namespace, self, actions_dict, idx_actions, actions_object)

        rate.sleep()

    ros_server.stop()
