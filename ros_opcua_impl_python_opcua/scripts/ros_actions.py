# !/usr/bin/python
# thanks to https://github.com/ros-visualization/rqt_common_plugins/blob/groovy-devel/rqt_action/src/rqt_action/action_plugin.py
import string
import random
import pydoc

import rospy
import rostopic
import roslib
from roslib import message
import actionlib
import actionlib_msgs

from opcua import ua, common
from opcua import uamethod
from opcua.ua.uaerrors import UaError

import ros_server
import ros_services
import ros_topics
import ros_utils


class OpcUaROSAction:

    def __init__(self, server, parent, idx, action_name, type_name):
        self.server = server
        self.idx = idx

        self.name = action_name

        self.goal_type, self.goal_name, self.goal_fn = rostopic.get_topic_type(self.name + "/goal")
        self.feedback_type, self.feedback_name, self.feedback_fn = rostopic.get_topic_type(self.name + "/feedback")
        self.result_type, self.result_name, self.result_fn = rostopic.get_topic_type(self.name + "/result")

        rospy.logdebug("Action name: " + self.name)
        rospy.logdebug("Goal type: " + self.goal_type)
        rospy.logdebug("Feedback type: " + self.feedback_type)
        rospy.logdebug("Result type: " + self.result_type)

        self.type = self.goal_type.replace("Goal", "")
        self.type = self.feedback_type.replace("Feedback", "")
        self.type = self.result_type.replace("Result", "")

        self._feedback_nodes = {}

        # goal_name = "_" + action_type.split("/")[-1]
        # msg_name = goal_name.replace("Goal", "")
        # class_name = msg_name.replace("_", "", 1)

        rospy.logdebug("Trying to find module with name: " + "%s.msg.%s" % tuple(self.type.split('/')))
        action_class = pydoc.locate("%s.msg.%s" % tuple(self.type.split('/')))

        rospy.logdebug("We are creating action: " + self.name)
        rospy.logdebug("We have type: " + self.type)
        # rospy.logdebug("We have msg name: " + msg_name)
        # rospy.logdebug("We have class name: " + class_name)
        # rospy.logdebug("We have goal name: " + goal_name)
        # rospy.logdebug("We have goal class name: " + goal_name.replace("_", "", 1))

        # self.goal_class = pydoc.locate("%s.msg.%s" % tuple(self.goal_type.split('/')))
        # self.feedback_class = pydoc.locate("%s.msg.%s" % tuple(self.feedback_type.split('/')))
        # self.result_class = pydoc.locate("%s.msg.%s" % tuple(self.result_type.split('/')))

        try:
            self.goal_class = roslib.message.get_message_class(self.goal_type)
            rospy.logdebug("Found '%s' message class: '%s'", self.goal_type, str(self.goal_class))
        except (ValueError, TypeError):
            rospy.logerr("Can't find message class for '%s'", self.goal_type)

        try:
            self.feedback_class = roslib.message.get_message_class(self.feedback_type)
            rospy.logdebug("Found '%s' message class: '%s'", self.feedback_type, str(self.feedback_class))
        except (ValueError, TypeError):
            rospy.logerr("Can't find message class for '%s'", self.feedback_type)

        try:
            self.result_class = roslib.message.get_message_class(self.result_type)
            rospy.logdebug("Found '%s' message class: '%s'", self.result_type, str(self.result_class))
        except (ValueError, TypeError):
            rospy.logerr("Can't find message class for '%s'", self.result_type)

        # try:
        #     self.goal_class = getattr(goal_spec, self.goal_type.split('/')[1])
        #     self.feedback_class = getattr(feedback_spec, self.feedback_type.split('/')[1])
        #     self.result_class = getattr(result_spec, self.result_type.split('/')[1])
        # except AttributeError as ex:
        #     rospy.logerr(ex)
        #     return

        # malformed move_base_simple Action hack
        # if 'move_base_simple' in self.name:
        #     self.goal_instance = self.goal_class()
        # else:
        #     self.goal_instance = self.goal_class().goal
        # rospy.logdebug("found goal_instance " + str(self.goal_instance))


        try:
            self.client = actionlib.SimpleActionClient(self.get_ns_name(), action_class)
            rospy.logdebug("Created SimpleActionClient for action: '%s'", self.name)
        except actionlib.ActionException as ex:
            rospy.logerr("Error while creating SimpleActionClient for action: '%s'", self.name, ex)


        rospy.logdebug("Creating parent objects for action: '%s'", self.name)

        self.parent = self.recursive_create_objects(self.name, self.idx, parent)
        rospy.logdebug("Found parent for action: " + str(self.parent))
        rospy.logdebug("Creating main node with name: " + self.name.split('/')[-1])


        # parent is our main node, this means our parent in log message above was actionsObject
        if self.name.split("/")[-1] == self.parent.nodeid.Identifier:
            self.main_node = self.parent
        else:
            self.main_node = self.parent.add_object(
                ua.NodeId(self.name.split("/")[-1], self.parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                ua.QualifiedName(self.name.split("/")[-1], self.parent.nodeid.NamespaceIndex))
        rospy.logdebug("Created Action object node: '%s'", self.name)


        # Action Goal OPC-UA Object
        rospy.logdebug("Creating goal object node: '%s'", self.name + "/goal")
        # self.goal_object = self.main_node.add_object(
        #     ua.NodeId(self.name + "/goal", self.main_node.nodeid.NamespaceIndex, ua.NodeIdType.String),
        #     ua.QualifiedName("goal", parent.nodeid.NamespaceIndex))
        self._recursive_create_items(self.main_node, self.name + "/goal", self.goal_type, self.goal_class())
        # self.goal_node = self.goal_object.add_method(idx, self.name + "_send_goal", self.send_goal, getargarray(self.goal_instance), [])

        # Action Feedback OPC-UA Object
        rospy.logdebug("Creating feedback object node: '%s'", self.name + "/feedback")
        # self.feedback_object = self.main_node.add_object(
        #     ua.NodeId(self.name + "feedback", self.main_node.nodeid.NamespaceIndex, ua.NodeIdType.String),
        #     ua.QualifiedName("feedback", self.main_node.nodeid.NamespaceIndex))
        self._recursive_create_items(self.main_node, self.name + "/feedback", self.feedback_type, self.feedback_class())

        # Action Result OPC-UA Object
        rospy.logdebug("Creating result object node: '%s'", self.name + "/result")
        # self.result_object = self.main_node.add_object(
        #     ua.NodeId(self.name + "/result", self.main_node.nodeid.NamespaceIndex, ua.NodeIdType.String),
        #     ua.QualifiedName("result", self.main_node.nodeid.NamespaceIndex))
        self._recursive_create_items(self.main_node, self.name + "/result", self.result_type, self.result_class())

        # Action Status OPC-UA Object
        rospy.logdebug("Creating status object node: '%s'", self.name + "/status")
        # self.status_object = self.main_node.add_object(
        #     ua.NodeId(self.name + "status", self.main_node.nodeid.NamespaceIndex, ua.NodeIdType.String),
        #     ua.QualifiedName("status", self.main_node.nodeid.NamespaceIndex))
        self._recursive_create_items(self.main_node, self.name + "/status", "actionlib_msgs/GoalStatusArray", actionlib_msgs.msg.GoalStatusArray())
        # self.status_node = ros_topics._create_node_with_type(self.status, self.idx, self.name + "_status", self.name + "_status", "string", -1)

        # Action Cancel OPC-UA Object
        rospy.logdebug("Creating cancel object node: '%s'", self.name + "/cancel")
        # self.cancel_object = self.main_node.add_object(
        #     ua.NodeId(self.name + "cancel", self.main_node.nodeid.NamespaceIndex, ua.NodeIdType.String),
        #     ua.QualifiedName("cancel", self.main_node.nodeid.NamespaceIndex))
        self._recursive_create_items(self.main_node, self.name + "/cancel", "actionlib_msgs/GoalID", actionlib_msgs.msg.GoalID())
        # self.goal_cancel = self.goal.add_method(idx, self.name + "_cancel_goal", self.cancel_goal, [], [])

        rospy.loginfo("Created ROS Action with name: %s", self.name)


    def message_callback(self, message):
        self.update_value(self.name + "/feedback", message)


    def update_value(self, topic_name, message):
        if hasattr(message, '__slots__') and hasattr(message, '_slot_types'):
            for slot_name in message.__slots__:
                self.update_value(topic_name + '/' + slot_name, getattr(message, slot_name))

        elif type(message) in (list, tuple):
            if (len(message) > 0) and hasattr(message[0], '__slots__'):
                for index, slot in enumerate(message):
                    if topic_name + '[%d]' % index in self._feedback_nodes:
                        self.update_value(topic_name + '[%d]' % index, slot)
                    else:
                        if topic_name in self._feedback_nodes:
                            base_type_str, array_size = ros_utils.extract_array_info(self._feedback_nodes[topic_name].text(self.feedback_type))
                            self._recursive_create_items(self._feedback_nodes[topic_name], topic_name + '[%d]' % index, base_type_str, slot)
            # remove obsolete children
            if topic_name in self._feedback_nodes:
                if len(message) < len(self._feedback_nodes[topic_name].get_children()):
                    for i in range(len(message), self._feedback_nodes[topic_name].childCount()):
                        item_topic_name = topic_name + '[%d]' % i
                        self.recursive_delete_items(self._feedback_nodes[item_topic_name])
                        del self._feedback_nodes[item_topic_name]
        else:
            if topic_name in self._feedback_nodes and self._feedback_nodes[topic_name] is not None:
                self._feedback_nodes[topic_name].set_value(repr(message))

    @uamethod
    def cancel_goal(self, parent, *inputs):
        # rospy.logdebug("cancelling goal " + self.name)
        try:
            self.client.cancel_all_goals()
            self.update_state()
        except (rospy.ROSException, UaError) as e:
            rospy.logerr("Error when cancelling a goal for " + self.name, e)


    def recursive_create_objects(self, topic_name, idx, parent):
        rospy.logdebug("reached parent object creation! current parent: " + str(parent))
        hierachy = topic_name.split('/')
        rospy.logdebug("Current hierachy: " + str(hierachy))
        if len(hierachy) == 0 or len(hierachy) == 1:
            return parent
        for name in hierachy:
            rospy.logdebug("current name: " + str(name))
            if name != '':
                try:
                    node_with_same_name = self.server.find_action_node_with_same_name(name, idx)

                    if node_with_same_name is not None:
                        rospy.logdebug("Found node with same name, is now new parent")
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, node_with_same_name)
                    else:
                        # if for some reason 2 services with exactly same name are created use hack>: add random int, prob to hit two
                        # same ints 1/10000, should be sufficient
                        child = parent.add_object(
                            ua.NodeId(name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                            ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                        return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, child)
                # thrown when node with parent name is not existent in server
                except IndexError, UaError:
                    child = parent.add_object(
                        ua.NodeId(name + str(random.randint(0, 10000)), parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                        ua.QualifiedName(name, parent.nodeid.NamespaceIndex))
                    return self.recursive_create_objects(ros_server.nextname(hierachy, hierachy.index(name)), idx, child)

        return parent


    def _recursive_create_items(self, parent, topic_name, type_name, msg):
        idx = self.idx
        topic_text = topic_name.split('/')[-1]
        if '[' in topic_text:
            topic_text = topic_text[topic_text.index('['):]

        rospy.logdebug("create item '%s'", topic_name)

        if hasattr(msg, '__slots__') and hasattr(msg, '_slot_types'):
            # complex type
            node = parent.add_object(
                ua.NodeId(topic_name, parent.nodeid.NamespaceIndex, ua.NodeIdType.String),
                ua.QualifiedName(topic_text, parent.nodeid.NamespaceIndex))

            for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
                self._recursive_create_items(node, topic_name + '/' + slot_name, slot_type, getattr(msg, slot_name))
            # self._feedback_nodes[feedback_topic_name] = new_node

        else:
            # array or simple type
            base_type_str, array_size = ros_utils.extract_array_info(type_name)

            try:
                base_class = roslib.message.get_message_class(base_type_str)
                base_instance = base_class()
            except (ValueError, TypeError):
                base_instance = None

            if array_size is not None and hasattr(base_instance, '__slots__'):
                # complex type array
                for index in range(array_size):
                    self._recursive_create_items(parent, topic_name + '[%d]' % index, base_type_str, base_instance)
            else:
                # simple type or simple type array
                node = ros_topics.create_topic_variable(parent, idx, topic_name, topic_text, type_name)
                node.set_writable(True)
                # self._feedback_nodes[feedback_topic_name] = node
        return

    # namespace
    def get_ns_name(self):
        splits = self.name.split("/")
        ns = splits[1:]
        res = ""
        for split in ns:
            res += split + "/"
        rospy.logdebug("Created ns name: " + res[:-1])
        return str(res[:-1])

    @uamethod
    def send_goal(self, parent, *inputs):
        rospy.loginfo("Sending Goal for " + self.name)
        try:
            goal_msg = self.create_message_instance(inputs, self.goal_instance)
            if 'move_base' in self.name:
                rospy.loginfo("setting frame_id for move_base malformation")
                try:
                    target_pose = getattr(goal_msg, "target_pose")
                    header = getattr(target_pose, "header")
                    setattr(header, "frame_id", "map")
                    setattr(target_pose, "header", header)
                except AttributeError as e:
                    rospy.logerr("Error occured when setting frame_id", e)
            rospy.loginfo("Created Message Instance for goal-send: " + str(goal_msg))
            self.client.send_goal(goal_msg, done_cb=self.update_result, feedback_cb=self.update_feedback,
                                  active_cb=self.update_state)
            return
        except Exception as e:
            rospy.logerr("Error occured during goal sending for Action " + str(self.name))
            print(e)


    def create_message_instance(self, inputs, sample):
        rospy.logdebug("Creating message for goal call")
        already_set = []
        if isinstance(inputs, tuple):
            arg_counter = 0
            object_counter = 0
            while arg_counter < len(inputs) and object_counter < len(sample.__slots__):
                cur_arg = inputs[arg_counter]
                cur_slot = sample.__slots__[object_counter]
                # ignore header for malformed move_base_goal, as header shouldnt be in sent message
                while cur_slot == 'header':
                    rospy.logdebug("ignoring header")
                    object_counter += 1
                    if object_counter < len(sample.__slots__):
                        cur_slot = sample.__slots__[object_counter]
                real_slot = getattr(sample, cur_slot)
                rospy.lodebug(
                    "cur_arg: " + str(cur_arg) + " cur_slot_name: " + str(cur_slot) + " real slot content: " + str(
                        real_slot))
                if hasattr(real_slot, '_type'):
                    rospy.logdebug("We found an object with name " + str(cur_slot) + ", creating it recursively")
                    arg_counter_before = arg_counter
                    already_set, arg_counter = self.create_object_instance(already_set, real_slot, cur_slot,
                                                                           arg_counter, inputs, sample)
                    if arg_counter != arg_counter_before:
                        object_counter += 1
                    rospy.logdebug("completed object, object counter: " + str(object_counter) + " len(object): " + str(
                        len(sample.__slots__)))
                else:
                    already_set.append(cur_slot)
                    # set the attribute in the request
                    setattr(sample, cur_slot, cur_arg)
                    arg_counter += 1
                    object_counter += 1

        return sample


    def create_object_instance(self, already_set, object, name, counter, inputs, parent):
        rospy.logdebug("Create Object Instance Notify")
        object_counter = 0
        while object_counter < len(object.__slots__) and counter < len(inputs):
            cur_arg = inputs[counter]
            cur_slot = object.__slots__[object_counter]
            # ignore header for malformed move_base_goal, as header shouldnt be in sent message
            while cur_slot == 'header':
                rospy.logdebug("ignoring header")
                object_counter += 1
                if object_counter < len(object.__slots__):
                    cur_slot = object.__slots__[object_counter]
                else:
                    return already_set, counter
            real_slot = getattr(object, cur_slot)
            rospy.logdebug(
                "cur_arg: " + str(cur_arg) + " cur_slot_name: " + str(cur_slot) + " real slot content: " + str(
                    real_slot))
            if hasattr(real_slot, '_type'):
                rospy.logdebug("Recursive Object found in request/response of service call")
                already_set, counter = self.create_object_instance(already_set, real_slot, cur_slot, counter, inputs,
                                                                   object)
                object_counter += 1
            else:
                already_set.append(cur_slot)
                setattr(object, cur_slot, cur_arg)
                object_counter += 1
                counter += 1
                # sets the object as an attribute in the request were trying to build
        setattr(parent, name, object)
        return already_set, counter


    def recursive_delete_items(self, item):
        self.client.cancel_all_goals()
        for child in item.get_children():
            self.recursive_delete_items(child)
            self.server.server.delete_nodes([child])
        self.server.server.delete_nodes([self.result, self.result_node, self.goal_node, self.goal, self.parent])
        ros_server.own_rosnode_cleanup()


    def update_result(self, state, result):
        rospy.logdebug("updated result cb reached")
        self.status_node.set_value(map_status_to_string(state))
        self.result_node.set_value(repr(result))


    def update_state(self):
        rospy.logdebug("updated state cb reached")
        self.status_node.set_value(repr(self.client.get_goal_status_text()))


    def update_feedback(self, feedback):
        rospy.logdebug("updated feedback cb reached")
        self.message_callback(feedback)


def get_correct_name(topic_name):
    rospy.logdebug("getting correct name for: " + str(topic_name))
    splits = topic_name.split('/')
    return string.join(splits[:-1], '/')


def getargarray(goal_class):
    array = []
    for slot_name in goal_class.__slots__:
        if slot_name != 'header':
            slot = getattr(goal_class, slot_name)
            if hasattr(slot, '_type'):
                array_to_merge = getargarray(slot)
                array.extend(array_to_merge)
            else:
                if isinstance(slot, list):
                    rospy.logdebug("Found an Array Argument!")
                    arg = ua.Argument()
                    arg.Name = slot_name
                    arg.DataType = ua.NodeId(ros_services.getobjectidfromtype("array"))
                    arg.ValueRank = -1
                    arg.ArrayDimensions = []
                    arg.Description = ua.LocalizedText("Array")
                else:
                    arg = ua.Argument()
                    if hasattr(goal_class, "__name__"):
                        arg.Name = goal_class.__name__ + slot_name
                    else:
                        arg.Name = slot_name
                    arg.DataType = ua.NodeId(ros_services.getobjectidfromtype(type(slot).__name__))
                    arg.ValueRank = -1
                    arg.ArrayDimensions = []
                    arg.Description = ua.LocalizedText(slot_name)
                array.append(arg)

    return array


def map_status_to_string(param):
    if param == 9:
        return "Goal LOST"
    elif param == 8:
        return "Goal RECALLED"
    elif param == 7:
        return "Goal RECALLING"
    elif param == 6:
        return "Goal PREEMPTING"
    elif param == 5:
        return "Goal REJECTED"
    elif param == 4:
        return "Goal ABORTED"
    elif param == 3:
        return "Goal SUCEEDED"
    elif param == 2:
        return "Goal PREEMPTED"
    elif param == 1:
        return "Goal ACTIVE"
    elif param == 0:
        return "Goal PENDING"


def refresh_dict(ros_namespace, actions_dict, server, idx_actions):
    # get current published topics
    topics = rospy.get_published_topics(ros_namespace)

    to_be_deleted = []
    for opcua_action_name in actions_dict:
        found = False
        for topic_name, topic_type in topics:
            ros_server.own_rosnode_cleanup()
            if opcua_action_name in topic_name:
                found = True
        if not found:
            actions_dict[opcua_action_name].recursive_delete_items(actions_dict[opcua_action_name].parent)
            to_be_deleted.append(opcua_action_name)
            rospy.logdebug("Deleting OPC-UA action: " + opcua_action_name)
            ros_server.own_rosnode_cleanup()
    for name in to_be_deleted:
        del actions_dict[name]


def refresh_actions(ros_namespace, ros_server, actions_dict, idx_actions, actions_object):
    ros_topics = rospy.get_published_topics(ros_namespace)

    # rospy.logdebug(str(ros_topics))
    # rospy.logdebug(str(rospy.get_published_topics('/move_base_simple')))

    for topic_name, topic_type in ros_topics:

        action_name = get_correct_name(topic_name)

        if action_name not in actions_dict or actions_dict[action_name] is None:
            splits = topic_name.split('/')

            if splits[-1] in ["status", "cancel", "goal", "feedback", "result"]:
                # rospy.loginfo("Ignoring normal topics for debugging...")
                opcua_action = OpcUaROSAction(ros_server, actions_object, idx_actions, action_name, topic_type)
                actions_dict[topic_name] = opcua_action
            else:
                rospy.logdebug("Found a Topic: %s", str(topic_name))
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

        # elif number_of_subscribers(topic_name, topics_dict) <= 1 and "rosout" not in topic_name:
        #     topics_dict[topic_name].recursive_delete_items(server.server.get_node(ua.NodeId(topic_name, idx_topics)))
        #     del topics_dict[topic_name]
        #     ros_server.own_rosnode_cleanup()

    refresh_dict(ros_namespace, actions_dict, ros_server, idx_actions)


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
