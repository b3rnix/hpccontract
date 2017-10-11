import argparse
import sys
from dbsession import *


def get_create_hpc_node_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", type = str, help = 'Cluster the node belongs to')
    parser.add_argument("nodeid", type = str, help = 'Unique id of the node within the cluster')
    parser.add_argument("hostname", type = str, help = 'Node\'s host name')
    parser.add_argument("capacity", type = int, help = 'Number of core hours per hour (typically number of cores)')
    return parser

def get_create_entity_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("entityid", type = str, help = 'Id of entity')
    parser.add_argument("entityname", type = str, help = 'Descriptive name')
    parser.add_argument("entitytype", type = str, help = 'Entity type (person, project, organizarion, etc.)')

    return parser

def get_create_hpc_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Contract if')
    parser.add_argument("description", type = str, help = 'Descriptive name')
    parser.add_argument("uri", type = str, help = 'Contract\'s URI')
    parser.add_argument("--startdate", type = int, help = 'Date the contract is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Contract\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the contract is active', default=True)


    return parser

def get_assign_node_to_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodeid", type = str, help = 'Unique id of the node within the cluster')
    parser.add_argument("contractid", type = str, help = 'Contract if')
    parser.add_argument("share", type = str, help = 'Resources transfered from the node to the contract. The only values currently supported are percentages (a string like 20%)')
    parser.add_argument("--startdate", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_sub_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("childcontractid", type = str, help = 'Child contract')
    parser.add_argument("parentcontractid", type = str, help = 'Parent contract')
    parser.add_argument("share", type = str, help = 'Resources transfered from the parent contract to the child contract. The only values currently supported are percentages (a string like 20%)')
    parser.add_argument("--startdate", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_contract_administrator_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Administered contract')
    parser.add_argument("entityid", type = str, help = 'Entity who administers the contract')
    parser.add_argument("--startdate", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_contract_owner_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Owned contract')
    parser.add_argument("entityid", type = str, help = 'Entity who is owner of the contract')

    return parser


parser_builders = {}
parser_builders['createhpcnode'] = get_create_hpc_node_parser
parser_builders['createentity'] = get_create_entity_parser
parser_builders['createhpccontract'] = get_create_hpc_contract_parser
parser_builders['assignnodetocontract'] = get_assign_node_to_contract_parser
parser_builders['assignsubcontract'] = get_assign_sub_contract_parser
parser_builders['assigncontractadministrator'] = get_assign_contract_administrator_parser
parser_builders['assigncontractowner'] = get_assign_contract_owner_parser





def exec_create_hpc_node(args):
    create_hpc_node(args.cluster, args.nodeid, args.hostname, args.capacity)

def exec_create_entity(args):
    create_entity(args.entityid, args.entityname, args.entitytype)

def exec_create_hpc_contract(args):
    create_hpc_contract(args.contractid, args.description, args.uri, args.startdate, args.enddate, args.active)

def exec_assign_node_to_contract(args):
    assign_node_to_contract(args.nodeid, args.contractid, args.share, args.startdate, args.enddate,args.active)

def exec_assign_sub_contract(args):
    assign_sub_contract(args.childcontractid, args.parentcontractid, args.share, args.startdate, args.enddate, args.active)

def exec_assign_contract_administrator(args):
    assign_contract_administrator(args.contractid, args.entityid, args.startdate, args.enddate, args.active)

def exec_assign_contract_owner(args):
    assign_contract_owner(args.contractid, args.entityid, args.startdate, args.enddate, args.active)


command_funcs = {}

command_funcs['createhpcnode'] = exec_create_hpc_node
command_funcs['createentity'] = exec_create_entity
command_funcs['createhpccontract'] = exec_create_hpc_contract
command_funcs['assignnodetocontract'] = exec_assign_node_to_contract
command_funcs['assignsubcontract'] = exec_assign_sub_contract
command_funcs['assigncontractadministrator'] = exec_assign_contract_administrator
command_funcs['assigncontractowner'] = exec_assign_contract_owner


def get_parser(main_command):
    return parser_builders[main_command]()

### Main program ###

main_command = sys.argv[1]
sys.argv.remove(main_command)

valid_commands = ['createhpcnode','createentity','createhpccontract','assignnodetocontract', 'assignsubcontract', 'assigncontractadministrator', 'assigncontractowner']
if main_command not in valid_commands:
    print "Invalid command (case sensitive): ", main_command, 'Valid commands are', valid_commands
    exit(1)

args = get_parser(main_command).parse_args()

command_funcs[main_command](args)




