#!/home/berna/anaconda2/bin/python
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

def get_create_nas_node_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", type = str, help = 'Cluster the NAS belongs to')
    parser.add_argument("nodeid", type = str, help = 'Unique id of the NAS within the cluster')
    parser.add_argument("hostname", type = str, help = 'NAS\'s host name')
    parser.add_argument("capacity", type = int, help = 'Final number of GB de NAS provides')
    return parser


def get_create_cluster_user_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", type = str, help = 'Cluster the User belongs to')
    parser.add_argument("userid", type = str, help = 'Unique id of the NAS within the cluster')
    parser.add_argument("email", type = str, help = 'NAS\'s host name')
    parser.add_argument("--startdate", type = int, help = 'Date the user account is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Accounts\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the user account is active', default=True)

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

def get_create_nas_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Contract id')
    parser.add_argument("description", type = str, help = 'Descriptive name')
    parser.add_argument("uri", type = str, help = 'Contract\'s URI')
    parser.add_argument("--startdate", type = int, help = 'Date the contract is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Contract\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the contract is active', default=True)


    return parser


def get_update_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Contract id')
    parser.add_argument("--description", type = str, help = 'Descriptive name', default=None)
    parser.add_argument("--uri", type = str, help = 'Contract\'s URI', default=None)
    parser.add_argument("--startdate", type = int, help = 'Date the contract is valid from', default=None)
    parser.add_argument("--enddate", type = int, help = 'Contract\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the contract is active', default=None)


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


def get_link_contracts_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("srccontractid", type = str, help = 'The contract that gives resources')
    parser.add_argument("dstcontractid", type = str, help = 'The contract that receives resources')
    parser.add_argument("share", type = str, help = 'Resources transfered from the source contract to the destination contract. The only values currently supported are percentages (a string like 20%)')
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

def get_assign_contract_user():
    parser = argparse.ArgumentParser()
    parser.add_argument("contractid", type = str, help = 'Administered contract')
    parser.add_argument("userid", type = str, help = 'Entity who becomes user of the specified contract')
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
parser_builders['createnasnode'] = get_create_nas_node_parser
parser_builders['createentity'] = get_create_entity_parser
parser_builders['createhpccontract'] = get_create_hpc_contract_parser
parser_builders['createnascontract'] = get_create_nas_contract_parser
parser_builders['assignnodetocontract'] = get_assign_node_to_contract_parser
parser_builders['assignsubcontract'] = get_assign_sub_contract_parser
parser_builders['assigncontractadministrator'] = get_assign_contract_administrator_parser
parser_builders['assigncontractowner'] = get_assign_contract_owner_parser
parser_builders['updatecontract'] = get_update_contract_parser
parser_builders['linkcontracts'] = get_link_contracts_parser
parser_builders['assignuser2contract'] = get_assign_contract_user
parser_builders['createclusteruser'] = get_create_cluster_user_parser


def exec_create_hpc_node(args):
    create_hpc_node(args.cluster, args.nodeid, args.hostname, args.capacity)

def exec_create_nas_node(args):
    create_hpc_node(args.cluster, args.nodeid, args.hostname, args.capacity)

def exec_create_cluster_user(args):
    create_cluster_user(args.cluster, args.userid, args.email, args.startdate, args.enddate, args.active)

def exec_create_entity(args):
    create_entity(args.entityid, args.entityname, args.entitytype)

def exec_create_hpc_contract(args):
    create_hpc_contract(args.contractid, args.description, args.uri, args.startdate, args.enddate, args.active)

def exec_create_nas_contract(args):
    create_nas_contract(args.contractid, args.description, args.uri, args.startdate, args.enddate, args.active)

def exec_assign_node_to_contract(args):
    assign_node_to_contract(args.nodeid, args.contractid, args.share, args.startdate, args.enddate,args.active)

def exec_assign_sub_contract(args):
    assign_sub_contract(args.childcontractid, args.parentcontractid, args.share, args.startdate, args.enddate, args.active)

def exec_assign_contract_administrator(args):
    assign_contract_administrator(args.contractid, args.entityid, args.startdate, args.enddate, args.active)

def exec_assign_contract_owner(args):
    assign_contract_owner(args.contractid, args.entityid, args.startdate, args.enddate, args.active)

def exec_update_contract(args):
    update_contract(id=args.contractid, description=args.description, uri=args.uri, start_date=args.startdate, end_date=args.enddate, active=args.active )

def exec_link_contracts(args):
    link_contracts_by_use(resource_contract_id=args.srccontractid, consumer_contract_id=args.dstcontractid, share=args.share, start_date=args.startdate, end_date=args.enddate, active=args.active)


def exec_assign_contract_user(args):
    assign_contract_user(args.contractid, args.userid, args.startdate, args.enddate, args.active)



command_funcs = {}

command_funcs['createhpcnode'] = exec_create_hpc_node
command_funcs['createnasnode'] = exec_create_nas_node
command_funcs['createentity'] = exec_create_entity
command_funcs['createhpccontract'] = exec_create_hpc_contract
command_funcs['createnascontract'] = exec_create_nas_contract
command_funcs['assignnodetocontract'] = exec_assign_node_to_contract
command_funcs['assignsubcontract'] = exec_assign_sub_contract
command_funcs['assigncontractadministrator'] = exec_assign_contract_administrator
command_funcs['assigncontractowner'] = exec_assign_contract_owner
command_funcs['updatecontract'] = exec_update_contract
command_funcs['linkcontracts'] = exec_link_contracts
command_funcs['assignuser2contract'] = exec_assign_contract_user
command_funcs['createclusteruser'] = exec_create_cluster_user

def get_parser(main_command):
    return parser_builders[main_command]()

### Main program ###

valid_commands = command_funcs.keys()

if len(sys.argv) < 2:
    print "Must specify a command. Valid commands: ", valid_commands
    exit(1)
	
main_command = sys.argv[1]
sys.argv.remove(main_command)

if main_command not in valid_commands:
    print "Invalid command (case sensitive): ", main_command, 'Valid commands are', valid_commands
    exit(1)

args = get_parser(main_command).parse_args()

command_funcs[main_command](args)




