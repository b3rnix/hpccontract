from api import *
from py.templateHelper import run_template


def get_data():
    contracts = get_contracts_list(contract_type='HPC')
    props = filter (lambda v: 'uri' in v.keys(), contracts.values())
    uridata = [get_slurm_data_from_uri(v['uri']) for v in props]

    clusters = {}


    for u in uridata:
        c = u['cluster']
        p = u['partition']

        if not c in clusters.keys():
            clusters[c] = {}

        if not p in clusters[c]:
            clusters[c][p] = []

        if 'account' in u.keys():
            if not u['account'] in clusters[c][p]:
                clusters[c][p].append(u['account'])

    return clusters


print run_template(template_name="generate_slurm_partitions_file", get_data_func=get_data)