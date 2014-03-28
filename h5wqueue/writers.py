__author__ = 'victor'

from redis import Redis
from rq import Queue
import tables

red_conn = Redis()
writer_q = Queue('write', connection=red_conn)


def create_node_task(h5file_name, path, node_name, node_type="group", attributes=None, array=None, compress=False):
    """
    This function creates a task in the write queue of a redis database, use throw rq
    :param h5file_name: Name of the file. it will always be opened with "a" option for not blocking reading from the
    same file.
    :param path: path to the parent node inside de h5 file.
    :param node_name: Name of the new node
    :param node_type: Type of node. It accepts group or array as values. If it is not defined group is taken as default.
    :param attributes: Attribute of the nodes. It should be a dictionary where keys are the name of the attributes and
    the values are their associated values.
    :param array: Whenever an Array has to be saved this is the array that will be written.
    :param compress: Boolean for if the array should be compress or not.
    :return: Return the rq task.
    """

    if attributes is None:
        attributes = {}
    args = (h5file_name, path, node_name)
    kwargs = {"node_type": node_type, "attributes": attributes, "array": array, "compress": compress}
    return writer_q.enqueue_call(func=create_node, args=args, kwargs=kwargs)


def create_node(h5file_name, path, node_name, node_type="group", attributes=None, array=None, compress=False):
    """
    This function creates a node in an h5 file.
   :param h5file_name: Name of the file. it will always be opened with "a" option for not blocking reading from the
    same file.
    :param path: path to the parent node inside de h5 file.
    :param node_name: Name of the new node
    :param node_type: Type of node. It accepts group or array as values. If it is not defined group is taken as default.
    :param attributes: Attribute of the nodes. It should be a dictionary where keys are the name of the attributes and
    the values are their associated values.
    :param array: Whenever an Array has to be saved this is the array that will be written.
    :param compress: Boolean for if the array should be compress or not.
    """
    if attributes is None:
        attributes = {}
    h5 = tables.open_file(h5file_name, "a")
    try:
        if node_type.lower().strip() == "group":
            new_node = h5.create_group(path, node_name, createparents=True)
        elif node_type.lower().strip() == "array":
            if array is None:
                raise Exception("Cannot create an Array with a NoneType Object.")
            if compress:
                new_node = h5.create_carray(path, node_name, createparents=True, obj=array)
            else:
                new_node = h5.create_array(path, node_name, createparents=True, obj=array)
        else:
            raise AttributeError("node_type parameter must be 'group' or 'array'")
        for atrr, value in attributes.items():
            h5.set_node_attr(new_node, atrr, value)
    finally:
        print "Closing open file"
        h5.close()


def modify_attr_task(h5file_name, node_path, set_attributes=None, delete_attributes=()):
    """
    Function for crete a task that modifies the attributes in a h5 files node
    :param h5file_name: Name of the file. it will always be opened with "a" option for not blocking reading from the
    same file.
    :param node_path: Path to the note
    :param set_attributes: Dictionary containing pair of names and values for attributes to be set
    :param delete_attributes: Iterable containing the attributes to be deleted.
    :return: return the task created
    """
    if set_attributes is None:
        set_attributes = {}
    args = (h5file_name, node_path)
    kwargs = {"set_attributes": set_attributes, "delete_attributes": delete_attributes}
    return writer_q.enqueue_call(func=modify_attributes, args=args, kwargs=kwargs)


def modify_attributes(h5file_name, node_path, set_attributes=None, delete_attributes=()):
    """
    Function that modifies the attributes in a h5 files node
    :param h5file_name: Name of the file. it will always be opened with "a" option for not blocking reading from the
    same file.
    :param node_path: Path to the note
    :param set_attributes: Dictionary containing pair of names and values for attributes to be set
    :param delete_attributes: Iterable containing the attributes to be deleted.
    """
    if set_attributes is None:
        set_attributes = {}
    h5 = tables.open_file(h5file_name, "a")
    try:
        node = h5.get_node(node_path)
        for name, value in set_attributes.items():
            h5.set_node_attr(node_path, name, value)
        for attr in delete_attributes:
            h5.del_node_attr(node_path, attr)
    finally:
        h5.close()

