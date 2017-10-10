import argparse
import sys


def get_create_hpc_node_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", type = str, help = 'Cluster the node belongs to')
    parser.add_argument("--node-id", type = str, help = 'Unique id of the node within the cluster')
    parser.add_argument("--host-name", type = str, help = 'Node\'s host name')
    parser.add_argument("--capacity", type = int, help = 'Number of core hours per hour (typically number of cores)')
    return parser

def get_create_entity_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity-id", type = str, help = 'Id of entity')
    parser.add_argument("--entity-name", type = str, help = 'Descriptive name')
    parser.add_argument("--entity-type", type = str, help = 'Entity type (person, project, organizarion, etc.)')

    return parser

def get_create_hpc_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", type = str, help = 'Contract if')
    parser.add_argument("--description", type = str, help = 'Descriptive name')
    parser.add_argument("--uri", type = str, help = 'Contract\'s URI')
    parser.add_argument("--start-date", type = int, help = 'Date the contract is valid from', default=None)
    parser.add_argument("--end-date", type = int, help = 'Contract\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the contract is active', default=True)


    return parser

def get_assign_node_to_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type = str, help = 'Unique id of the node within the cluster')
    parser.add_argument("--contract-id", type = str, help = 'Contract if')
    parser.add_argument("--share", type = str, help = 'Resources transfered from the node to the contract. The only values currently supported are percentages (a string like 20%)')
    parser.add_argument("--start-date", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--end-date", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_sub_contract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--child-contract-id", type = str, help = 'Child contract')
    parser.add_argument("--parent-contract-id", type = str, help = 'Parent contract')
    parser.add_argument("--share", type = str, help = 'Resources transfered from the parent contract to the child contract. The only values currently supported are percentages (a string like 20%)')
    parser.add_argument("--start-date", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--end-date", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_contract_administrator_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", type = str, help = 'Administered contract')
    parser.add_argument("--entity-id", type = str, help = 'Entity who administers the contract')
    parser.add_argument("--start-date", type = int, help = 'Date the link is valid from', default=None)
    parser.add_argument("--end-date", type = int, help = 'Link\'s due date', default=None)
    parser.add_argument("--active", type = bool, help = 'True/False. Defaults to True. Weather the link is active', default=True)


    return parser

def get_assign_contract_owner_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", type = str, help = 'Owned contract')
    parser.add_argument("--entity-id", type = str, help = 'Entity who is owner of the contract')

    return parser


parser_builders = {}
parser_builders['create-hpc-node'] = get_create_hpc_node_parser
parser_builders['create-entity'] = get_create_entity_parser
parser_builders['create-hpc-contract'] = get_create_hpc_contract_parser
parser_builders['assign-node-to-contract'] = get_assign_node_to_contract_parser
parser_builders['assign-sub-contract'] = get_assign_sub_contract_parser
parser_builders['assign-contract-administrator'] = get_assign_contract_administrator_parser
parser_builders['assign-contract-owner'] = get_assign_contract_owner_parser

def get_parser(main_command):
    return parser_builders[main_command]()


def get_args():

    main_command = sys.argv[1]
    
    if main_command not in ['create_hpc_node','create_entity','create_hpc_contract','assign_node_to_contract', 'assign_sub_contract', 'assign_contract_administrator', 'assign_contract_owner']:
        print "Invalid command (case sensitive): ", main_command
        exit(1)

    return main_command, get_parser(main_command).parse_args




