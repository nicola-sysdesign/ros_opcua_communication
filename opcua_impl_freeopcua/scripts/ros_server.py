#!/usr/bin/env python

# Thanks to: https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_topic/src/rqt_topic/topic_widget.py

import rospy
import roslib, roslib.message

import datetime
import time
import sys

from IPython import embed
import opcua


class OpcUaROSTopic():
    
    def __init__(self, parent, idx, topic_name, topic_type):
        
        self.type_name = topic_type
        self.name = topic_name
        
        self._nodes = {}
        
        self.message_class = None
        try:
            self.message_class = roslib.message.get_message_class(topic_type)
            self.message_instance = self.message_class()
        except Exception as e:
            self.message_class = None
            rospy.logfatal("There is not found message type class " + topic_type)
        
        self._recursive_create_items(parent, idx, topic_name, topic_type, self.message_instance)
        
    def _recursive_create_items(self, parent, idx, topic_name, type_name, message):
        topic_text = topic_name.split('/')[-1]
        if '[' in topic_text:
            topic_text = topic_text[topic_text.index('['):]
       
        # This here are 'complex data'
        if hasattr(message, '__slots__') and hasattr(message, '_slot_types'):
            new_node = parent.add_object(idx, topic_text)
            new_node.add_property(idx, "Type", type_name)
            for slot_name, type_name in zip(message.__slots__, message._slot_types):
                self._recursive_create_items(new_node, idx, topic_name + '/' + slot_name, type_name, getattr(message, slot_name))

        else:
            # This are arrays
            base_type_str, array_size = self._extract_array_info(type_name)
            try:
                base_instance = roslib.message.get_message_class(base_type_str)()
            except (ValueError, TypeError):
                base_instance = None
            
            if array_size is not None and hasattr(base_instance, '__slots__'):
                for index in range(array_size):
                    self._recursive_create_items(parent, idx, topic_name + '[%d]' % index, base_type_str, base_instance)
            else:
                new_node = self._create_node_with_type(parent, idx, topic_text, type_name, array_size)
        
        self._nodes[topic_name] = new_node
        
        return
    
    def _extract_array_info(self, type_str):
        array_size = None
        if '[' in type_str and type_str[-1] == ']':
            type_str, array_size_str = type_str.split('[', 1)
            array_size_str = array_size_str[:-1]
            if len(array_size_str) > 0:
                array_size = int(array_size_str)
            else:
                array_size = 0

        return type_str, array_size
    
    def _create_node_with_type(self, parent, idx, topic_text, type_name, array_size):
        
        node = None
        
        if '[' in type_name:
            type_name = type_name[:type_name.index('[')]
        
        if type_name == 'bool':
            value = False
        elif type_name == 'byte':
            value = 0
        elif type_name == 'int8':
            value = 0
        elif type_name == 'uint8':
            value = 0
        elif type_name == 'int16':
            value = 0
        elif type_name == 'uint16':
            value = 0
        elif type_name == 'int32':
            value = 0
        elif type_name == 'uint32':
            value = 0
        elif type_name == 'int64':
            value = 0
        elif type_name == 'uint64':
            value = 0
        elif type_name == 'float':
            value = 0.0
        elif type_name == 'double':
            value = 0.0
        elif type_name == 'string':
            value = ''
        else:
            print type_name
            return None
        
        if array_size is not None: value=[value for i in range(array_size)]
        return parent.add_variable(idx, topic_text, value)


def main(args):
    
    rospy.init_node("opcua_server")
    
    server = opcua.Server(False)
    server.set_endpoint("opc.tcp://localhost:21554/")
    server.set_server_name("ROS OpcUa Server")
    
    server.start()
    
    try:
        # setup our own namespace, this is expected
        uri = "http://ros.org"
        idx = server.register_namespace(uri)
   
        # get Objects node, this is where we should put our custom stuff 
        objects = server.get_objects_node()
        
        #TODO: Add refresh method (namespace can be sent as paramter)
        
        topics = objects.add_object(idx, "ROS-Topics")
        #TODO: Add reading of system state
        
        ros_topics = rospy.get_published_topics()
        
        for topic_name, topic_type in ros_topics:            
            OpcUaROSTopic(topics, idx, topic_name, topic_type)
            
        rospy.spin()
        
    except rospy.ROSInterruptException:
        
        server.stop() 


if __name__ == "__main__":
    main(sys.argv)