from api import *
from py.templateHelper import run_template


def get_data():

    # DATAFORMAT : CLUSTER -> ACTIVE    [BANKING ACCOUNTS] -> ADD    -> [USER ACCOUNTS]
    #                                                      -> DELETE -> [USER ACCOUNTS]
    # DATAFORMAT : CLUSTER -> INACTIVE  [BANKING ACCOUNTS] -> ADD    -> [USER ACCOUNTS]
    #                                                      -> DELETE -> [USER ACCOUNTS]

    clusters = {}

    for s in ['ACTIVE', 'INACTIVE']:
        contracts = get_contracts_list(contract_type='HPC', state=s)
        props = filter (lambda v: 'uri' in v.keys(), contracts.values())
        for p in props:
            uridata = get_slurm_data_from_uri(p['uri'])

            c = uridata['cluster']
            id = p['id']

            if not c in clusters.keys():
                clusters[c] = {'ACTIVE': {}, 'INACTIVE': {}}
            if 'account' in uridata.keys():
                account = uridata['account']
                clusters[c][s][account] = {
                    'ACTIVE':  [u['id'] for u in get_contract_users(contract_id=id,state='ACTIVE')],
                    'INACTIVE': [u['id'] for u in get_contract_users(contract_id=id, state='INACTIVE')]
                }



    return clusters


print run_template(template_name="generate_slurm_banking_accounts", get_data_func=get_data)