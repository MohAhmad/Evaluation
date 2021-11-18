#!/usr/bin/python
import argparse
import json

import os
from random import randrange

import requests

from time import sleep
from typing import List

from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.net import Containernet, Controller
from mininet.node import Node
# TCLinks need to be imported after Node, due to Mininet's cyclic deps
from mininet.link import TCLink
from requests import Response
import time
import certifi
from io import BytesIO
import pycurl
import json
from io import StringIO


def build_topo(net: Containernet, depth: int, fanout: int,
               image="mohahmad1/nebulastream:test"):
    net.hostNum = 1
    net.switchNum = 1
    net.image = image
    net.current_worker_port = 3000  # start from this one and onwards
    net.worker_ports = [3000, 3001]  # set of ports needing exposure outside container
    net.max_depth = depth
    crd = _create_new_coordinator(net)
    _create_tree(net, depth=depth, fanout=fanout)
    #print("node name: ",crd.name, "    sw name: ",net.switches[0].name, "    Delay:",_create_delay_with_depth(depth), "  bw: ", _create_bandwidth_with_depth(depth))
    net.addLink(crd, net.switches[0], cls=TCLink, delay=_create_delay_with_depth(depth), bw = _create_bandwidth_with_depth(depth))


def _create_tree(net: Containernet, depth: int, fanout: int):
    """Add a subtree starting with node n.
               returns: last node added"""
    isSwitch = depth > 0
    if isSwitch:
        node = net.addSwitch('sw%s' % net.switchNum)
        net.switchNum += 1

        if depth == net.max_depth:
            child = net.addSwitch('sw%s' % net.switchNum)
            net.switchNum += 1
            net.addLink(node, child, cls=TCLink)
            node = child
            depth -= 1
        if depth > 1:
            gateway_node = _create_new_nes_node(net, node_name='gateway')
            #print("node name: ",node.name, "    gateway_node name: ",gateway_node.name, "    Delay:",_create_delay_with_depth(depth), "  bw: ", _create_bandwidth_with_depth(depth))
            net.addLink(node, gateway_node, delay = _create_delay_with_depth(depth), bw = _create_bandwidth_with_depth(depth))	
            gateway_node2 = _create_new_nes_node(net, node_name='gateway')
            #print("node name: ",node.name, "    gateway_node name: ",gateway_node.name, "    Delay:",_create_delay_with_depth(depth), "  bw: ", _create_bandwidth_with_depth(depth))
            net.addLink(node, gateway_node2, delay = _create_delay_with_depth(depth), bw = _create_bandwidth_with_depth(depth))

        for _ in range(fanout):
            child = _create_tree(net, depth - 1, fanout=fanout)
            if child.name.startswith('sw') and node.name.startswith('sw'):
                net.addLink(node, child, cls=TCLink)
            else:
                #print("child name: ",child.name, "    node name: ",node.name, "    Delay:",_create_delay_with_depth(depth), "  bw: ", _create_bandwidth_with_depth(depth))
                net.addLink(node, child, cls=TCLink, delay=_create_delay_with_depth(depth), bw = _create_bandwidth_with_depth(depth))
    else:
        node = _create_new_nes_node(net)
    return node


def _create_new_nes_node(net: Containernet, node_name='worker') -> Node:
    node = net.addDocker('%s' % (node_name + str(net.hostNum)),
                         ports=[3000, 3001],
                         port_bindings={net.current_worker_port: 3000, net.current_worker_port + 1: 3001},
                         dimage=net.image)
    net.hostNum += 1
    net.current_worker_port += 2
    return node


def _create_new_coordinator(net: Containernet) -> Node:
    node = net.addDocker('crd', ip='10.0.0.1',
                        ports=[8081, 12346, 4000, 4001, 4002],
                        port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002},
                        dimage=net.image)

    return node


def _create_bandwidth_with_depth(depth: int) -> float:

    return 0.2 * depth**2
    


def _create_delay_with_depth(depth: int) -> str:

    ret_num = 90 - 10*depth
    return str(ret_num) + 'ms'


def get_worker_nodes(net: Containernet) -> List[Node]:
    return list(filter(lambda host_node: 'worker' in host_node.name, net.hosts))


def get_gateway_nodes(net: Containernet) -> List[Node]:
    return list(filter(lambda host_node: 'gateway' in host_node.name, net.hosts))


def get_all_nes_nodes(net: Containernet) -> List[Node]:
    gateway_nodes = get_gateway_nodes(net)
    worker_nodes = get_worker_nodes(net)
    return gateway_nodes + worker_nodes


def get_coordinator(net: Containernet) -> Node:
    return net.getNodeByName('crd')


def get_topology(net: Containernet) -> Response:
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)
    return requests.get('http://' + ip + ":8081/v1/nes/topology")

def add_airquality_schema(net: Containernet) -> str:
    buffer = BytesIO()
    url = 'http://0.0.0.0:8081/v1/nes/streamCatalog/addLogicalStream'
    data_dict = {'streamName' : 'air', 'schema':'Schema::create()->addField(createField(\"date\", UINT64))->addField(createField(\"time\", UINT64))->addField(createField(\"CO\",  FLOAT32))->addField(createField(\"PT08_S1\", FLOAT32))->addField(createField(\"NMHC\", FLOAT32))->addField(createField(\"C6H6\",  FLOAT32))->addField(createField(\"PT08_S2\", FLOAT32))->addField(createField(\"NOx\", FLOAT32))->addField(createField(\"PT08_S3\", FLOAT32))->addField(createField(\"NO2\", FLOAT32))->addField(createField(\"PT08_S4\", FLOAT32))->addField(createField(\"PT08_S5\", FLOAT32))->addField(createField(\"T\", FLOAT32))->addField(createField(\"RH\",  FLOAT32))->addField(createField(\"AH\", FLOAT32));'}
    crl = pycurl.Curl()
    crl.setopt(pycurl.URL, url)
    crl.setopt(pycurl.WRITEDATA, buffer)
    crl.setopt(pycurl.POST, 1)
    crl.setopt(pycurl.CAINFO, certifi.where())

    body_as_json_string = json.dumps(data_dict) # dict to json
    body_as_file_object = StringIO(body_as_json_string)
    crl.setopt(pycurl.READDATA, body_as_file_object)
    crl.setopt(pycurl.POSTFIELDSIZE, len(body_as_json_string))
    crl.perform()
    crl.close()
    body = buffer.getvalue()
    return body

def execute_query(net: Containernet) -> Response:
    headers = {'content-type': 'application/json'}
    query_dict = {'userQuery':'Query::from(\"air\").filter(Attribute(\"CO\")>=1.49).sink(FileSinkDescriptor::create(\"/opt/local/nebula-stream/data/output.csv\",\"CSV_FORMAT\",\"APPEND\"));', 'strategyName': 'BottomUp'}
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)
    return requests.post(url='http://' + ip + ":8081/v1/nes/query/execute-query",
                         data=json.dumps(query_dict), headers=headers)


def get_query_status(net: Containernet) -> str:
    headers = {'content-type': 'application/json'}
    params = {"queryId":"1"}
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)
    status = requests.get(url='http://' + ip + ":8081/v1/nes/queryCatalog/status",
                        params = {"queryId":"1"} , headers=headers)
    return status.json()['status']


def get_num_proccessed_buffers(net: Containernet) -> str:

    headers = {'content-type': 'application/json'}
    params = {"queryId":"1"}
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)
    return requests.get(url='http://' + ip + ":8081/v1/nes/queryCatalog/getNumberOfProducedBuffers",
                        params = {"queryId":"1"} , headers=headers)


def get_docker_external_ip(node: Node) -> str:
    return node.dcinfo['NetworkSettings']['Networks']['bridge']['IPAddress']


def start_nes_processes(net: Containernet) -> bool:
    cmd_prefix = '/entrypoint-containernet.sh /opt/local/nebula-stream/nesWorker --coordinatorPort=4000 \
                  --coordinatorIp='
    cmd_own_ip = ' --localWorkerIp='
    cmd_suffix_1 = ' --sourceType=MQTTSource --sourceConfig="tcp://172.17.0.2:1883;'
    cmd_suffix_2 = ';mohammad;airQuality;CSV;2;false;1000" \
                   --numberOfTuplesToProducePerBuffer=10 --numberOfBuffersToProduce=0  --sourceFrequency=100 \
                  --physicalStreamName=test_stream --logicalStreamName=air'
    crd = get_coordinator(net)
    crd.cmd(
        '/entrypoint-containernet.sh /opt/local/nebula-stream/nesCoordinator --coordinatorIp=10.0.0.1 --restIp=0.0.0.0 && cd /opt/local/nebula-stream && mkdir data && cd /opt/local/nebula-stream/data && touch output.csv')
    sleep(2)
    info('*** Add Air Quality schema to NES \n')
    response_message = add_airquality_schema(net)
    print(response_message)

    workers = get_worker_nodes(net)
    for worker in workers:
        worker.cmd(cmd_prefix + crd.IP() + cmd_own_ip + worker.IP() + cmd_suffix_1+str(worker.name)+cmd_suffix_2)
        print("Started worker: ",str(worker.name))
        sleep(1)
    gateways = get_gateway_nodes(net)
    for gateway in gateways:
       # print(cmd_prefix + crd.IP() + cmd_own_ip + gateway.IP()+'\n')
        gateway.cmd(cmd_prefix + crd.IP() + cmd_own_ip + gateway.IP())
        print("Started gateway: ",str(gateway.name))
        sleep(1)
    return True


def start_test_executing_valid_user_query_with_file_output_two_worker(net: Containernet, file_output: str) -> Response:
    """
    From testExecutingValidUserQueryWithFileOutputTwoWorker:
    ss << "{\"userQuery\" : ";
    ss << "\"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\"";
    ss << outputFilePath;
    ss << "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"";
    ss << "));\",\"strategyName\" : \"BottomUp\"}";
    ss << endl;
    """
    headers = {'content-type': 'application/json'}
    user_query_str_prefix = "\"Query::from(\\\"default_logical\\\").sink(FileSinkDescriptor::create(\\\""
    user_query_str_suffix = "\\\", \\\"CSV_FORMAT\\\", \\\"APPEND\\\"));"
    query_dict = {"userQuery": user_query_str_prefix + file_output + user_query_str_suffix,
                  "strategyName": "BottomUp"}
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)
    return requests.post(url='http://' + ip + ":8081/v1/nes/query/execute-query",
                         data=json.dumps(query_dict), headers=headers)

def set_parent_child_relationship(net: Containernet) -> bool:
    hosts = net.hosts
    parent_layer_size = 2
    children_layer_size = 4
    current_parent_id = 2
    current_child_id = 4
    headers = {'content-type': 'application/json'}
    url_addParent = 'http://0.0.0.0:8081/v1/nes/topology/addParent'
    url_removeParent = 'http://0.0.0.0:8081/v1/nes/topology/removeParent'
    parent_dict = {}
    crd = get_coordinator(net)
    ip = get_docker_external_ip(crd)

    while (parent_layer_size + children_layer_size < len(hosts)):
        current_host_index = parent_layer_size + children_layer_size
        for i in range(current_parent_id, current_parent_id + parent_layer_size):
            for j in range(2):
                parent_dict['parentId'] = '1'
                parent_dict['childId'] = str(current_child_id)
                response = requests.post(url=url_removeParent,
                data=json.dumps(parent_dict), headers=headers)
                if not response:
                    return False

                parent_dict['parentId'] = str(current_parent_id)
                response = requests.post(url=url_addParent,
                data=json.dumps(parent_dict), headers=headers)
                if not response:
                    return False
                current_child_id += 1
                sleep(1)
            current_parent_id += 1
        parent_layer_size = children_layer_size
        children_layer_size *= 2
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--depth",
                        help="The depth of the network tree",
                        default=2,
                        type=int)
    parser.add_argument("--fanout",
                        help="The number of leaves per node",
                        default=10,
                        type=int)
    parser.add_argument("--sleep",
                        help="How much to sleep while waiting for the topology",
                        default=60,
                        type=int)
    args = parser.parse_args()

    depth = args.depth  # depth of tree
    fanout = args.fanout  # num of nodes at gateway switches (+1 for gateway nodes)
    sleep_time = args.sleep or 100

    setLogLevel('info')


    info('*** Starting net\n')
    net = Containernet(controller=Controller)
    net.addController('c0')
    build_topo(net, depth=depth, fanout=fanout)
    net.start()
    info('*** Starting NES processes\n')
    started = start_nes_processes(net)
    if started:
        info('*** NES processes successfully started!\n')
    else:
        info('*** Could not start NES processes!\n')

    sleep(10)

    added_topology = set_parent_child_relationship(net)
    if added_topology:
        info('*** successfully added parent_child relationship to topology nodes\n')
    else:
        info('*** Error adding parent_child relationship to topology nodes\n')

    sleep(10)
    info('*** Submit the query\n')
    response = execute_query(net)
    if response:
        print(response.content)
        status = get_query_status(net)
        while (status != "RUNNING"):
            sleep(1)
            print("Status: ", status)
            get_query_status(net)
        print("The query is Running!")
        t_end = time.time() + 20
        while time.time() < t_end:
            result = get_num_proccessed_buffers(net)
            print(result.content)
            sleep(1)

    info('*** Monitoring time has ended\n')
    info('*** Running CLI\n')
    CLI(net)

    info('*** Stopping network')
    net.stop()
