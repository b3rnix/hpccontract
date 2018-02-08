from api import *
from py.templateHelper import run_template


def get_data():

    parts = {'ACTIVE': get_hpc_partitions_for_nodes(cluster='neurus',state='ACTIVE'),
             'INACTIVE': get_hpc_partitions_for_nodes(cluster='neurus', state='INACTIVE')
             }
    return {

        'partitions': parts
    }


print run_template(template_name="generate_node_rocks_attr", get_data_func=get_data)