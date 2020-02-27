# ROS
import rospy
# python-opcua
from opcua import ua


def extract_array_info(type_str):
    """
    Given a the type name
    return the base type
    and eventyally the array size
    """
    array_size = None
    if '[' in type_str and type_str[-1] == ']':
        type_str, array_size_str = type_str.split('[', 1)
        array_size_str = array_size_str[:-1]
        if len(array_size_str) > 0:
            array_size = int(array_size_str)
        else:
            array_size = 0

    return type_str, array_size


def ros_msg_to_arguments(msg):
    args = []
    for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
        rospy.logdebug("converting slot name: '%s' of type '%s' to argument.", slot_name, slot_type)
        slot_value = getattr(msg, slot_name)
        if hasattr(slot_value, '_type'):
            args_to_merge = ros_msg_to_arguments(slot_value)
            args.extend(args_to_merge)
        else:
            arg = slot_msg_to_argument(slot_name, slot_type)
            args.append(arg)

    return args


def slot_msg_to_argument(slot_name, slot_type):

    arg = ua.Argument()
    arg.Name = slot_name
    arg.Description = ua.LocalizedText(slot_name)

    base_type, array_size = extract_array_info(slot_type)

    if base_type in ['bool']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Boolean, 0)
    elif base_type in ['int8']:
        arg.DataType = ua.NodeId(ua.ObjectIds.SByte, 0)
    elif base_type in ['byte', 'uint8']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Byte, 0)
    elif base_type in ['int16']:
        rospy.roswarn("'int16' type is deprecated.")
        arg.DataType = ua.NodeId(ua.ObjectIds.Int16, 0)
    elif base_type in ['uint16']:
        arg.DataType = ua.NodeId(ua.ObjectIds.UInt16, 0)
    elif base_type in ['int', 'int32']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Int32, 0)
    elif base_type in ['uint32']:
        arg.DataType = ua.NodeId(ua.ObjectIds.UInt32, 0)
    elif base_type in ['int64']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Int64, 0)
    elif base_type in ['uint64']:
        arg.DataType = ua.NodeId(ua.ObjectIds.UInt64, 0)
    elif base_type in ['float', 'float32', 'float64']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Float, 0)
    elif base_type in ['double']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Double, 0)
    elif base_type in ['string']:
        arg.DataType = ua.NodeId(ua.ObjectIds.String, 0)
    elif base_type in ['Time', 'time']:
        arg.DataType = ua.NodeId(ua.ObjectIds.Time, 0)
    else:
        rospy.logerr("Can't create argument for slot '%s' of type '%s'", slot_name, slot_type)
        return None

    if array_size is None:
        arg.ValueRank = -1
        arg.ArrayDimensions = []
    else:
        arg.ValueRank = 1
        arg.ArrayDimensions = [array_size]

    return arg


def ros_msg_to_variants(msg):
    vars = []
    for slot_name, slot_type in zip(msg.__slots__, msg._slot_types):
        slot_value = getattr(msg, slot_name)
        if hasattr(slot_value, '_type'):
            vars_to_merge = ros_msg_to_variants(slot_value)
            vars.extend(vars_to_merge)
        else:
            var = slot_value_to_variant(slot_value, slot_type)
            vars.append(var)

    return tuple(vars)


def slot_value_to_variant(slot_value, slot_type):
    rospy.logdebug("converting value: %s of type '%s' to variant.", str(slot_value), slot_type)

    base_type, array_size = extract_array_info(slot_type)

    if base_type in ['bool']:
        var = ua.Variant(slot_value, ua.VariantType.Boolean)
    elif base_type in ['int8']:
        var = ua.Variant(slot_value, ua.VariantType.SByte)
    elif base_type in ['byte', 'uint8']:
        var = ua.Variant(slot_value, ua.VariantType.Byte)
    elif base_type in ['int16']:
        var = ua.Variant(slot_value, ua.VariantType.Int16)
    elif base_type in ['uint16']:
        var = ua.Variant(slot_value, ua.VariantType.UInt16)
    elif base_type in ['int', 'int32']:
        var = ua.Variant(slot_value, ua.VariantType.Int32)
    elif base_type in ['uint32']:
        var = ua.Variant(slot_value, ua.VariantType.UInt32)
    elif base_type in ['int64']:
        var = ua.Variant(slot_value, ua.VariantType.Int64)
    elif base_type in ['uint64']:
        var = ua.Variant(slot_value, ua.VariantType.UInt64)
    elif base_type in ['float', 'float32', 'float64']:
        var = ua.Variant(slot_value, ua.VariantType.Float)
    elif base_type in ['double']:
        var = ua.Variant(slot_value, ua.VariantType.Double)
    elif base_type in ['string']:
        var = ua.Variant(slot_value, ua.VariantType.String)
    else:
        rospy.logerr("Can't create variant for value: %s of type: %s", str(slot_value), slot_type)
        return None

    return var
