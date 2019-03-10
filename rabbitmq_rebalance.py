import logging
import sys
import time

HOST = "rabbitmq-host"
USER = "rabbituser"
PASSWORD = "rabbitpassword"


class RabbitQueue(object):
    """
    a small helper class to hold relevant queue info
    """

    def __init__(self, name, master, mirrors, message_bytes):
        self.name = name
        self.master = master
        self.mirrors = mirrors
        self.message_bytes = message_bytes


def _get_nodes():
    pass


def _get_queue():
    pass


def _get_queues():
    pass


def _create_policy(policy_name, pattern, priority, definition):
    pass


def _delete_policy(policy_name):
    pass


def _sync_queue(queue_name):
    pass


def _get_node_names():
    """
    returns the list of nodes in the cluster

    :return: the list of nodes in the cluster
    :rtype: list[str]
    """
    nodes = _get_nodes()
    node_list = []
    for n in nodes:
        node_list.append(n['name'])
    return node_list


def _get_queue_object(queue_json):
    """
    gets a single queue object from json
    :param dict queue_json: the queue json
    :rtype: RabbitQueue
    """
    q_name = queue_json['name']
    q_master = queue_json['node']
    q_slaves = queue_json.get('synchronised_slave_nodes', [])
    q_bytes = queue_json['message_bytes']
    return RabbitQueue(q_name, q_master, q_slaves, q_bytes)


def _get_queue_details():
    """
    returns a queues

    :rtype: RabbitQueue
    """
    q = _get_queue()
    return _get_queue_object(q)


def _get_queues_details():
    """
    returns the queues in the cluster
    :return: the queues in the cluster
    :rtype: list[RabbitQueue ]
    """
    queues = _get_queues()
    queue_list = []
    for q in queues:
        queue_object = _get_queue_object(q)
        queue_list.append(queue_object)
    return queue_list


def calc_new_dests():
    """
    calculates which queues need to move to what node.
    prefers to move queues which has synced mirror on dest node.

    :return: dict from dest_node to list of queues to move there
    :rtype: dict[str, list[RabbitQueue]]
    """
    hosts = {}
    for node in _get_node_names():
        hosts[node] = []
    for queue in _get_queues_details():
        hosts[queue.master].append(queue)
    avg = 0
    for host in hosts:
        avg = avg + len(hosts[host])
        logging.info("Found {} queues on host {}".format(len(hosts[host]), host))

    avg = int(avg / len(hosts))
    logging.info("Each node should have {} queues".format(avg))

    new_dests = {}
    for host_src in hosts:
        for host_dest in hosts:
            if host_src == host_dest:
                continue
            if host_dest not in new_dests:
                new_dests[host_dest] = []
            src_cnt = len(hosts[host_src])
            dest_cnt = len(hosts[host_dest] + new_dests[host_dest])
            move_cnt = max(0, min(src_cnt - avg, (dest_cnt - avg) * -1))
            if move_cnt > 0:
                hosts[host_src].sort(key=lambda x: x.message.bytes)  # sort this so the smaller queues are first
                right_mirror = [q for q in hosts[host_src] if host_dest in q.mirrors]
                logging.info("Need to move {} queues from {} to {}. "
                             "found {} queues with mirrors in dest".format(move_cnt, host_src, host_dest,
                                                                           len(right_mirror)))

                # first, move queues that has synced mirrors in dest.
                for i in range(min(move_cnt, len(right_mirror))):
                    new_dests[host_dest].append(right_mirror[i])
                    hosts[host_src].remove(right_mirror[i])
                    move_cnt -= 1
                    # if we still need to move queues, move the remaining amount
                if move_cnt > 0:
                    for i in range(move_cnt):
                        new_dests[host_dest].append(hosts[host_src].pop())
    return new_dests


def move_queue(queue, to_node):
    """
    moves a single queue to new master

    :param RabbitQueue queue: the queue to move
    :param str to_node: the new master node of the queue
    """

    queue_name = queue.name
    logging.info("moving queue {} to {}".format(queue_name, to_node))
    if queue.master == to_node:
        logging.info("Queue {} is already on node {}".format(queue_name, to_node))
        return
    policy_name = "MOVE_{}_to_{}".format(queue_name, to_node)
    definition_two_nodes = {
        "ha-mode": "nodes",
        "ha-params": [queue.master, to_node]
    }
    definition_one_node = {
        "ha-mode": "nodes",
        "ha-params": [to_node]
    }
    _create_policy(policy_name, "^{}$".format(queue_name), 990, definition_two_nodes)
    _sync_queue(queue_name)
    logging.info("Requested Queue {} To Be Synced".format(queue_name))
    try:
        cnt = 60
        while cnt >= 0:
            queue = _get_queue_details(queue_name)
            if queue.master == to_node or to_node in queue.mirrors:
                break
            logging.info(
                "Waiting for queue {} to sync slave to {}. remaining time: {}".format(queue_name, to_node, cnt))
            _sync_queue(queue_name)
            logging.info("Requested Queue {} To Be Synced".format(queue_name))
            time.sleep(1)
            cnt -= 1

        if cnt < 0:
            error = "Timed out on sync slave, queue: {}".format(queue_name)
            logging.error(error)
            raise Exception(error)
        _create_policy(policy_name, "^{}$".format(queue_name), 992, definition_one_node)
        _sync_queue(queue_name)
        logging.info("Requested Queue {} To Be Synced".format(queue_name))
        cnt = 60
        while cnt >= 0:
            queue = _get_queue_details(queue_name)
            if queue.master == to_node:
                break
            logging.info("Waiting for queue {} to move to {}. remaining time:{}".format(queue_name, to_node, cnt))
            _sync_queue(queue_name)
            logging.info("Requested Queue {} To Be Synced".format(queue_name))
            time.sleep(1)
            cnt -= 1
        if cnt < 0:
            error = "Timed out on moving queue: {}".format(queue_name)
            logging.error(error)
            raise Exception(error)

        logging.info("Queue {} moved to {} successfully".format(queue_name, to_node))
    finally:
        _delete_policy(policy_name)


def register_default_log_handler():
    """
    registers a console log handler on root logger
    """
    log_formatter = logging.Formatter("%(asctime)-15s [%(levelname)-8s] %(message)s")
    console_log_handler = logging.StreamHandler(sys.stdout)
    console_log_handler.setFormatter(log_formatter)
    logging.getLogger().addHandler(console_log_handler)
    logging.getLogger().setLevel(logging.INFO)


def balance_cluster():
    """
    balances the queues on the cluster
    """
    balance = calc_new_dests()
    for dst in balance:
        for q in balance[dst]:
            move_queue(q, dst)
