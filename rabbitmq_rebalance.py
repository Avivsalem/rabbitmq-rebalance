import json
import logging
import sys
import time
import urllib
import urlparse

import requests

HOST = "rabbitmq-host"
USER = "rabbituser"
PASSWORD = "rabbitpassword"
VHOST = "/"
SYNC_TIMEOUT = 60


class RabbitQueue(object):
    """
    a small helper class to hold relevant queue info
    """

    def __init__(self, name, master, mirrors, message_bytes):
        self.name = name
        self.master_node = master
        self.mirror_nodes = mirrors
        self.message_bytes = message_bytes


_base_url = 'http://{user}:{password}@{host}:15672/api/'.format(user=USER, password=PASSWORD, host=HOST)


def _get_nodes():
    """
    returns the list of nodes in the cluster
    :rtype: list[dict]
    """
    url = urlparse.urljoin(_base_url, 'nodes')
    response = requests.get(url)
    if not response.ok:
        error = "Can't get nodes. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)

    return json.loads(response.content)


def _get_queue(queue_name):
    """
    returns the queue details

    :param str queue_name: the name of the queue to get
    :rtype: dict
    """
    url = urlparse.urljoin(_base_url, 'queues/{vhost}/{name}'.format(
        vhost=urllib.quote_plus(VHOST),
        name=urllib.quote_plus(queue_name)
    ))
    response = requests.get(url)
    if not response.ok:
        error = "Can't get queue. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)

    return json.loads(response.content)


def _get_queues():
    """
    returns all the queue details for the queues on vhost

    :rtype: list[dict]
    """
    url = urlparse.urljoin(_base_url, 'queues/{vhost}'.format(vhost=urllib.quote_plus(VHOST)))
    response = requests.get(url)
    if not response.ok:
        error = "Can't get queues. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)

    return json.loads(response.content)


def _create_policy(policy_name, pattern, priority, definition):
    """
    creates a policy on the vhost

    :param str policy_name: the name of the policy
    :param str pattern: the pattern to match
    :param int priority: the priority of the policy
    :param dict definition: the policy definition
    """
    url = urlparse.urljoin(_base_url, 'policies/{vhost}/{name}'.format(
       vhost=urllib.quote_plus(VHOST),
       name=urllib.quote_plus(policy_name)
    ))

    data = {
       "pattern": pattern,
       "priority": priority,
       "apply-to": "queues",
       "definition": definition
    }

    response = requests.put(url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    if not response.ok:
        error = "Can't create policy. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)


def _delete_policy(policy_name):
    """
    deletes a policy

    :param str policy_name: the policy name
    """
    url = urlparse.urljoin(_base_url, 'policies/{vhost}/{name}'.format(
        vhost=urllib.quote_plus(VHOST),
        name=urllib.quote_plus(policy_name)
    ))

    response = requests.delete(url)
    if not response.ok:
        error = "Can't delete policy. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)


def _sync_queue(queue_name):
    """
    requests a queue to sync

    :param str queue_name: the queue name
    """
    url = urlparse.urljoin(_base_url, 'queues/{vhost}/{name}/actions'.format(
        vhost=urllib.quote_plus(VHOST),
        name=urllib.quote_plus(queue_name)
    ))

    data = {'action': 'sync'}

    response = requests.post(url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    if not response.ok:
        error = "Can't sync queue. error: {}".format(response.content)
        logging.error(error)
        raise Exception(error)


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


def _get_queue_details(queue_name):
    """
    returns a queues

    :param str queue_name: the name of the queue
    :rtype: RabbitQueue
    """
    q = _get_queue(queue_name)
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
    queues_on_node = {}
    # get all nodes in the cluster
    for node_name in _get_node_names():
        queues_on_node[node_name] = []

    # get queues for each node
    for queue in _get_queues_details():
        queues_on_node[queue.master_node].append(queue)

    # calc the avg amount of queues each node should have
    avg = 0
    for host, queues in queues_on_node.items():
        avg = avg + len(queues)
        logging.info("Found {} queues on host {}".format(len(queues), host))
    avg = int(avg / len(queues_on_node))
    logging.info("Each node should have {} queues".format(avg))

    new_destination_nodes = {}
    for source_node in queues_on_node:
        for destination_node in queues_on_node:

            if source_node == destination_node:  # no need to move a queue from node to itself
                continue

            if destination_node not in new_destination_nodes:
                new_destination_nodes[destination_node] = []

            source_queues_count = len(queues_on_node[source_node])
            destination_queues_count = len(queues_on_node[destination_node]) + \
                                       len(new_destination_nodes[destination_node])  # queues already assigned to be moved to destination node

            num_of_queues_to_move = max(0, min(source_queues_count - avg, (destination_queues_count - avg) * -1))

            if num_of_queues_to_move > 0:
                queues_on_node[source_node].sort(key=lambda x: x.message.bytes)  # sort this so smaller queues are first

                # find queues that are already mirrored on destination node
                already_mirrored = [q for q in queues_on_node[source_node] if destination_node in q.mirrors]
                logging.info("Need to move {} queues from {} to {}. "
                             "found {} queues with mirrors in dest".format(num_of_queues_to_move, source_node,
                                                                           destination_node,
                                                                           len(already_mirrored)))

                # first, move queues that has synced mirrors in dest.
                for i in range(min(num_of_queues_to_move, len(already_mirrored))):
                    new_destination_nodes[destination_node].append(already_mirrored[i])
                    queues_on_node[source_node].remove(already_mirrored[i])
                    num_of_queues_to_move -= 1

                # if we still need to move queues, move the remaining amount
                if num_of_queues_to_move > 0:
                    for i in range(num_of_queues_to_move):
                        new_destination_nodes[destination_node].append(queues_on_node[source_node].pop())

    return new_destination_nodes


def move_queue(queue, destination_node):
    """
    moves a single queue to new master

    :param RabbitQueue queue: the queue to move
    :param str destination_node: the new master node of the queue
    """

    queue_name = queue.name
    logging.info("moving queue {} to {}".format(queue_name, destination_node))

    if queue.master_node == destination_node:
        logging.info("Queue {} is already on node {}".format(queue_name, destination_node))
        return

    policy_name = "MOVE_{}_to_{}".format(queue_name, destination_node)
    try:
        # this policy forces the queue to be mirrored on current master, and destination node
        definition_two_nodes = {
            "ha-mode": "nodes",
            "ha-params": [queue.master_node, destination_node]
        }

        _create_policy(policy_name, "^{}$".format(queue_name), 990, definition_two_nodes)
        _sync_queue(queue_name)
        logging.info("Requested Queue {} To Be Synced".format(queue_name))

        # wait until queue is synced to new destination
        timeout = SYNC_TIMEOUT
        while timeout >= 0:
            queue = _get_queue_details(queue_name)
            if queue.master_node == destination_node or destination_node in queue.mirror_nodes:
                break
            logging.info(
                "Waiting for queue {} to sync slave to {}. remaining time: {}".format(queue_name, destination_node,
                                                                                      timeout))
            _sync_queue(queue_name)
            logging.info("Requested Queue {} To Be Synced".format(queue_name))
            time.sleep(1)
            timeout -= 1

        if timeout < 0:
            error = "Timed out on sync slave, queue: {}".format(queue_name)
            logging.error(error)
            raise Exception(error)

        # this policy forces the queue to move its master to the destination node
        definition_one_node = {
            "ha-mode": "nodes",
            "ha-params": [destination_node]
        }
        _create_policy(policy_name, "^{}$".format(queue_name), 992, definition_one_node)
        _sync_queue(queue_name)
        logging.info("Requested Queue {} To Be Synced".format(queue_name))

        # wait until queue moved to new destination
        timeout = SYNC_TIMEOUT
        while timeout >= 0:
            queue = _get_queue_details(queue_name)
            if queue.master_node == destination_node:
                break
            logging.info(
                "Waiting for queue {} to move to {}. remaining time:{}".format(queue_name, destination_node, timeout))
            _sync_queue(queue_name)
            logging.info("Requested Queue {} To Be Synced".format(queue_name))
            time.sleep(1)
            timeout -= 1

        if timeout < 0:
            error = "Timed out on moving queue: {}".format(queue_name)
            logging.error(error)
            raise Exception(error)

        logging.info("Queue {} moved to {} successfully".format(queue_name, destination_node))

    finally:  # makes sure the policy is deleted in the end.
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


if __name__ == "__main__":
    register_default_log_handler()
    balance_cluster()
