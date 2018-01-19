import datetime
from neo4j.v1 import GraphDatabase, basic_auth
import re
import config

driver = GraphDatabase.driver(config.neo4j_url, auth=basic_auth(config.neo4j_user, config.neo4j_passwd))
slurmuriregex  = re.compile('slurm:\/\/c:([a-zA-Z0-9]+)\/p:([a-zA-Z0-9]+)\/a:([a-zA-Z0-9]+)')
def run_query(query, pars):
    sess = driver.session()
    result = sess.run(query, pars)
    data = result.data()
    sess.close()
    return data


def get_slurm_data_from_uri(uri):
    m = slurmuriregex.match(uri)
    if m == None:
        raise Exception("Invalid SLURM URI!!")

    return {
        "cluster": m.groups()[0],
        "partition": m.groups()[1],
        "account": m.groups()[2]
    }

def check_no_overlapping_relationship(src_node, dst_node, relation_name, start_date=0, end_date=99999999, active=True):
    # Ojo, no chequea el cluster. Si hay dos nodos con el mismo Id en distinto cluster pegandole a otro nodo va a fallar la validacion!!
    query = "MATCH (y{id:'"+src_node+"'})-[r:" + str.upper(relation_name) + "]->(x{id:'"+dst_node+"'}) WHERE (((NOT exists(r.start_date) OR r.start_date <= {pstart_date}) AND (NOT exists(r.end_date) OR r.end_date >= {pstart_date})) OR ((NOT exists(r.start_date) OR r.start_date <= {pend_date}) AND (NOT exists(r.end_date) OR r.end_date >= {pend_date}))) AND (NOT exists(r.active) OR r.active) RETURN r "
    pars = {'pstart_date': start_date, 'pend_date': end_date}
    data = run_query(query, pars)
    if len(data) > 0:
        raise Exception("Relationship can not de added.  An active time-overlapping relationship of the same type and between the same nodes already exists. Consider disabling the conflicting relationship.")


def check_node_doesnt_exists(node_id, cluster_id):
    today_num = get_today_num()
    query = "MATCH (n{id:{pnode_id}}) WHERE (NOT exists(n.cluster) OR {pcluster_id} IS NULL OR n.cluster={pcluster_id}) AND (NOT exists(n.start_date) OR n.start_date <= {pdate}) AND (NOT exists(n.end_date) OR n.end_date > {pdate}) RETURN n"
    pars = {'pdate': today_num, 'pnode_id': node_id,'pcluster_id': cluster_id}
    data = run_query(query,pars)
    if len(data) > 0:
        raise Exception(
            "An active node with the same id already exists.")

def get_contract(contract_id, cluster_id=None):
    sess = driver.session()
    result = sess.run("MATCH (c:CONTRACT) WHERE c.id = {id} "
                           "RETURN c",
                           {"id": contract_id})

    data = result.data()
    sess.close()
    if (len(data) == 0):
        raise Exception("Unregistered contract")

    return data[0].values()[0]



def create_hpc_node(cluster, node_id, host_name, capacity):
    check_node_doesnt_exists(node_id, cluster)

    query = "CREATE (:AHPCNODE:HPC:NODE{cluster:{pcluster}, id:{pid}, host_name:{phost_name}, capacity:{pcapacity},uri:{puri}})"
    pars =  {
                "pcluster": cluster,
                "pid": node_id,
                "phost_name": host_name,
                "pcapacity": capacity,
                "puri": "slurm://c:{0}/n:{1}".format(cluster,node_id)
             }
    run_query(query=query, pars=pars)

def create_nas_node(cluster, node_id, host_name, capacity):
    check_node_doesnt_exists(node_id, cluster)

    query = "CREATE (:ANASNODE:NAS:NODE{cluster:{pcluster}, id:{pid}, host_name:{phost_name}, capacity:{pcapacity},uri:{puri}})"
    pars =  {
                "pcluster": cluster,
                "pid": node_id,
                "phost_name": host_name,
                "pcapacity": capacity,
                "puri": "nas://c:{0}/n:{1}".format(cluster,node_id)
             }

    run_query(query=query, pars=pars)

def create_nas_contract(id, description, uri, start_date=None, end_date=None, active=True):
    check_node_doesnt_exists(id, cluster=None)
    query = "CREATE (:ANASCONTRACT:CONTRACT:NAS{id:{pid}, description:{pdescription}, start_date:{pstart_date}, end_date:{pend_date}, uri:{puri}, active:{pactive}})"
    pars = {
                "pid": id,
                "pdescription": description,
                "pstart_date": start_date,
                "pend_date": end_date,
                "puri": uri,
                "pactive": active
             }

    run_query(query=query, pars=pars)


def create_entity(entity_id, entity_name, entity_type):
    sess = driver.session()
    result = sess.run("MATCH (n) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": entity_id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Entity already exists")


    sess.run(
            "CREATE (:AENTITY:" + entity_type + "{id:{pid}, name:{pname}})",
            {
                "pid": entity_id,
                "pname": entity_name
             }
             )

    sess.close()



def create_hpc_contract(id, description, uri, start_date=None, end_date=None, active=True):
    sess = driver.session()
    result = sess.run("MATCH (n:CONTRACT:HPC) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Contract already exists")


    sess.run(
            "CREATE (:AHPCCONTRACT:CONTRACT:HPC{id:{pid}, description:{pdescription}, start_date:{pstart_date}, end_date:{pend_date}, uri:{puri}, active:{pactive}})",
            {
                "pid": id,
                "pdescription": description,
                "pstart_date": start_date,
                "pend_date": end_date,
                "puri": uri,
                "pactive": active
             }
             )

    sess.close()

#create_hpc_contract(id="hpc_bancarizada",description="Contrato de Cuenta Bancarizada HPC para Neurus",start_date=20170701,uri="slurm://neurus/bancarizada", active=True )

def get_node(node_id):
    pass


def assign_hpc_node_to_contract(node_id, contract_id, share=None, start_date=None, end_date=None, active=True):

    get_contract(contract_id=contract_id)
    get_node(node_id=node_id)
    check_no_overlapping_relationship(src_node=node_id, dst_node=contract_id,relation_name="USES",start_date=start_date,end_date=end_date,active=active)

    query = "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (n:) WHERE n.id = {pnode_id} CREATE (c)-[:USES{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(n)"
    pars = {
            "pnode_id": node_id,
            "pcontract_id": contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

           }
    run_query(query,pars)


def assign_nas_node_to_contract(node_id, contract_id, share="100%", start_date=None, end_date=None, active=True):

    get_node(node_id)
    get_contract(contract_id)
    check_no_overlapping_relationship(src_node=contract_id, dst_node=node_id, relation_name="USES",
                                      start_date=start_date, end_date=end_date, active=active)


    run_query(
        query=
        "MATCH (c:CONTRACT:NAS) WHERE c.id = {pcontract_id} MATCH (n:NAS:NODE) WHERE n.id = {pnode_id} CREATE (c)-[:USES{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(n)",
        pars=
        {
            "pnode_id": node_id,
            "pcontract_id": contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )


def assign_sub_contract(child_contract_id, parent_contract_id, share=None, start_date=None, end_date=None, active=True):
    run_query(
        query=
        "MATCH (p:CONTRACT) WHERE p.id = {pparentcontract_id} MATCH (c:CONTRACT) WHERE c.id = {pchildcontract_id} CREATE (c)-[:SUBCONTRACT{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(p)",
        pars=
        {
            "pparentcontract_id": parent_contract_id,
            "pchildcontract_id": child_contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )


def assign_contract_administrator(contract_id, person_id, start_date=None, end_date=None, active=True):
    check_no_overlapping_relationship(src_node=person_id, dst_node=contract_id, relation_name="ADMINISTRATOR",start_date=start_date,end_date=end_date,active=active)
    run_query(
        query=
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:PERSON) WHERE p.id = {pperson_id} CREATE (p)-[:ADMINISTRATOR{start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(c)",
        pars=
        {
            "pcontract_id": contract_id,
            "pperson_id": person_id,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )


def assign_contract_owner(contract_id, entity_id, entity_type):
    check_no_overlapping_relationship(src_node=entity_id, dst_node=contract_id, relation_name="OWNER")
    run_query(
        query=
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:" + entity_type + ") WHERE p.id = {pentity_id} CREATE (p)-[:OWNER]->(c)",
        pars=
        {
            "pcontract_id": contract_id,
            "pentity_id": entity_id

        }
    )


def assign_contract_user(contract_id, entity_id, start_date=None, end_date=None, active=True):
    check_no_overlapping_relationship(src_node=entity_id, dst_node=contract_id, relation_name="USES",start_date=start_date,end_date=end_date,active=active)
    run_query(
        query=
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:" + "CLUSTERUSER" + ") WHERE p.id = {pentity_id} CREATE (p)-[:USES{start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(c)",
        pars=
        {
            "pcontract_id": contract_id,
            "pentity_id": entity_id,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active


        }
    )



def create_cluster_user(cluster, user_id, email, start_date=None, end_date=None, active=True):
    sess = driver.session()
    result = sess.run("MATCH (n:USER) WHERE n.id = {pid} AND n.cluster = {pcluster}"
                           "RETURN count(*)",
                           {"pid": user_id, "pcluster": cluster})

    if (result.peek()["count(*)"] > 0):
        raise Exception("User already exists")


    sess.run(
            "CREATE (:CLUSTERUSER{cluster:{pcluster}, id:{pid}, email:{pemail}, uri:{puri}, description:{pdescription},start_date:{pstart_date},end_date:{pend_date}, active:{pactive} })",
            {
                "pcluster": cluster,
                "pid": user_id,
                "pemail": email,
                "puri": "clusteruser://c:{0}/u:{1}".format(cluster,user_id),
                "pstart_date": start_date,
                "pend_date": end_date,
                "pdescription": "User {0} on cluster {1}".format(user_id, cluster),
                "pactive": active
             }
             )

    sess.close()



#Creates a USES relationship so source gives resources to dst_contract.   (destination_contract)-[USES]->(origin_contract)
def link_contracts_by_use(resource_contract_id, consumer_contract_id, share="100%", start_date=None, end_date=None, active=True):
    get_contract(resource_contract_id)
    get_contract(consumer_contract_id)
    check_no_overlapping_relationship(src_node=consumer_contract_id, dst_node=resource_contract_id, relation_name="USES",
                                      start_date=start_date, end_date=end_date, active=active)


    run_query(
        query=
        "MATCH (p:CONTRACT) WHERE p.id = {porigincontract_id} MATCH (c:CONTRACT) WHERE c.id = {pdestinationcontract_id} CREATE (c)-[:USES{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(p)",
        pars=
        {
            "porigincontract_id": resource_contract_id,
            "pdestinationcontract_id": consumer_contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )


def update_contract(id, **kwargs):
    sess = driver.session()
    result = sess.run("MATCH (n:CONTRACT) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": id})
    sess.close()
    if (result.peek()["count(*)"] == 0):
        raise Exception("Contract does not exist.")

    update_node(id, kwargs)

def update_node(id, kwargs):
    set_keys = map(lambda k: "SET n." + k + "={p" + k + "}" if not kwargs[k] is None else "", kwargs.keys())
    set_values = {}
    set_values["id"] = id
    for k in kwargs:
        if not kwargs[k] is None:
            set_values["p"+k] = kwargs[k]

    run_query("MATCH (n) WHERE n.id = {id} " + " ".join(set_keys), set_values)

def isActive(n):
    current_date = datetime.date.year * 10000 + datetime.date.month * 100 + datetime.date.day
    active = (not hasattr(n, 'start_date') or n.start_date >= current_date) and (not hasattr(n, 'end_date') or n.end_date <= current_date)
    active = active and (not hasattr(n, 'active') or n.active)
    return active

def is_resource_node(node):
    return '' in node['labels']


def is_percent_rel(rel):
    return (rel['properties']['share'].count('%') > 0 )


def get_capacity_from_tree(tree, node):
    if len(tree) == 0:
        return 0.


    if is_resource_node(node):
        capacity = node['properties']['capacity']
    else:
        capacity = 0.0
        nbs = node['neighbors']
        for k in nbs.keys():
            nb = nbs[k]
            #TODO: EXTRAER ESTA LOGICA EN UN METODO
            if is_percent_rel(nb):
                capacity = capacity + float(str.strip(str(nb['properties']['share']),'%')) / 100. * get_capacity_from_tree(tree, tree[k])
            else:
                capacity = capacity + float(nb['properties']['share'])

    return capacity


def get_tree_from_paths(paths):
    tree = {}
    for pp in paths:
        for p in pp.values():
            for n in p.nodes:
                if not tree.has_key(n.id):
                    node = {'properties': n.properties, 'labels': n.labels, 'neighbors': {}}
                    tree[n.id] = node

    for pp in paths:
        for p in pp.values():
            for r in p.relationships:
                tree[r.start]['neighbors'][r.end] = {'node': tree[r.end],'type': r.type, 'properties': r.properties}

    return tree

def get_unbroken_paths_to_nodes(contract_id):
    # All paths whose relationships and nodes are all active and not due
    today_num = get_today_num()
    query = "MATCH p=(x:CONTRACT{id:{pcontract_id}})-[*]->(n:NODE) WHERE (all(r in relationships(p) WHERE (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > {pdate}) AND r.active)) AND (all(n in nodes(p) WHERE (NOT exists(n.start_date) OR n.start_date <= {pdate}) AND (NOT exists(n.end_date) OR n.end_date > {pdate}) )) RETURN p"
    pars = {"pcontract_id": contract_id, "pdate": today_num}
    data = run_query(query=query, pars=pars)
    if len(data) == 0:
        return 0,[]
    return data[0]['p'].start.id, data


def get_contract_capacity(contract_id):
    id, paths = get_unbroken_paths_to_nodes(contract_id)
    tree = get_tree_from_paths(paths)
    capacity = get_capacity_from_tree(tree, tree[id])
    return capacity



def generate_slurm_credits_command(contract_id, days):

    contract = get_contract(contract_id)
    if not "uri" in contract.properties:
        raise Exception("A contract to generate a SLURM command must have a SLURM URI")

    uri = contract.properties["uri"]
    slurm_data = get_slurm_data_from_uri(uri)
    capacity = get_available_capacity_for_contract(contract_id=contract_id)
    credit = capacity * days * 24

    print "Command to assign credits to contract {0}".format(contract_id)
    print "sbank-reset -c {0} -a {1}".format(slurm_data["cluster"], slurm_data["account"])
    print "sbank-deposit -c {0} -a {1} -t {2}".format(slurm_data["cluster"], slurm_data["account"], credit)

    pass



# Consultas
def get_hpc_contract_credits(contract_id):
    id, paths = get_unbroken_paths_to_nodes(contract_id)
    if paths == []:
        c = 0
    else:
        tree = get_tree_from_paths(paths)
        if not tree == {}:
            c = get_capacity_from_tree(tree, tree[id])
        else:
            c = 0

    return {'credits': c}

def get_contracts_list(contract_type=None):
    today_num = get_today_num()

    type_prefix = ""
    if not contract_type is None:
        type_prefix = ":" + str.upper(contract_type)


    query = "MATCH (c:CONTRACT" + type_prefix +  ") WHERE (NOT exists(c.start_date) OR c.start_date <= {pdate}) AND (NOT exists(c.end_date) OR c.end_date > {pdate}) AND c.active  RETURN c"
    pars = {"pdate": today_num}

    data = run_query(query=query,pars=pars)
    ret = {}
    for cc in data:
        for c in cc.values():
            ret[c.properties['id']] = c.properties

    return ret

def get_credits_for_all_hpc_contracts():
    contracts = get_contracts_list("HPC")
    credits = {}
    for k in contracts.keys():
        credits[k] = get_hpc_contract_credits(k)['credits']


    return credits


def get_node_list(node_type):
    today_num = get_today_num()

    query = "MATCH (c:"+str.upper(node_type)+"NODE) WHERE (NOT exists(c.start_date) OR c.start_date <= {pdate}) AND (NOT exists(c.end_date) OR c.end_date > {pdate}) AND (NOT exists(c.active) OR c.active)  RETURN c"

    sess = driver.session()
    result = sess.run(query, {"pdate": today_num})
    data = result.data()
    sess.close()
    ret = {}
    for cc in data:
        for c in cc.values():
            ret[c.properties['id']] = c.properties

    return ret


def get_today_num():
    today = datetime.date.today()
    today_num = today.year * 10000 + today.month * 100 + today.day
    return today_num


def get_contracts_uri_for_nodes(cluster, node_id=None):
    today = get_today_num()
    if not node_id is None:
        query = "MATCH p=(n:{id:{pnode_id},cluster:{pcluster}})<-[*]-(x:CONTRACT:HPC) WHERE (all(r in relationships(p) WHERE (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > 20180101) AND r.active)) AND (all(n in nodes(p) WHERE (NOT exists(n.start_date) OR n.start_date <= 20180101) AND (NOT exists(n.end_date) OR n.end_date > 20180101) )) RETURN n.id,x.uri"
        params = {'pnode_id': node_id}
    else:
        query = "MATCH p=(n:{cluster:{pcluster}})<-[*]-(x:CONTRACT:HPC) WHERE (all(r in relationships(p) WHERE (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > 20180101) AND r.active)) AND (all(n in nodes(p) WHERE (NOT exists(n.start_date) OR n.start_date <= 20180101) AND (NOT exists(n.end_date) OR n.end_date > 20180101) )) RETURN n.id,x.uri"
        params = {}

    params['pcluster'] = cluster
    params['pdate'] = today
    data = run_query(query, params)

    ret = {}

    for d in data:
        if not ret.has_key(d['n.id']):
            ret[d['n.id']] = []

        if not d['x.uri'] is None:
            ret[d['n.id']].append(d['x.uri'])

    return ret

def get_hpc_partitions_for_nodes(cluster, node_id=None):
    uris = get_contracts_uri_for_nodes(cluster, node_id)

    partitions = {}

    for k in uris.keys():
        partitions[k] = []
        for u in uris[k]:
            part = get_slurm_data_from_uri(u)['partition']
            if not part is None and not part in partitions[k]:
                partitions[k].append(part)

    return partitions

def get_slurm_partition_users(partition):
    query = "MATCH (u:CLUSTERUSER)-[r:USES]->(c:HPC:CONTRACT) WHERE (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > {pdate}) AND (NOT exists(r.active) OR r.active) AND (c.uri =~ {puripat}) RETURN u,c,r"
    pars = {'pdate':get_today_num(),'puripat': '.*/p:' + partition + '.*'}
    data = run_query(query,pars)
    return [{'user': d['u'].properties, 'relation': d['r'].properties} for d in data]


#Rule: When a User as link to a NAS contract, then It will have R/W access to the resource pointed to that contract's
# URI. Access is granted by adding the user to a group, whose name is determined as a function of the URI.
def get_nas_contract_group_name(contract):
    return "nas_grp_" + str(contract.id)

def get_nas_group_members(cluster_id, contract_id):
    query = "MATCH (u:CLUSTERUSER)-[r:USES]->(c:CONTRACT:NAS) WHERE c.id = 'nas_neurus_gerencia1_grupoa' AND (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > {pdate}) AND (NOT exists(r.active) OR r.active) RETURN u"
    pars = {'pdate':get_today_num(), 'contract_id': contract_id}
    data = run_query(query, pars)
    return [d['u'].properties for d in data]


def get_nas_contract_user_group(cluster_id, contract_id):
    contract = get_contract(contract_id, cluster_id)
    group_name= get_nas_contract_group_name(contract)
    members = get_nas_group_members(cluster_id, contract_id)
    return {'group_name': group_name, 'members': members }


# Rule: ZFS Volumes are Second-Level contracts (NAS->TOP LEVEL CONTRACT-->VOLUMES)
def get_nas_volume_contracts(cluster_id, node_id):
    query = "MATCH p=((c:CONTRACT:NAS)-[*2..2]->(n:ANASNODE)) WHERE n.id = {pnode_id} AND (all(r in relationships(p) WHERE (NOT exists(r.start_date) OR r.start_date <= {pdate}) AND (NOT exists(r.end_date) OR r.end_date > {pdate}) AND r.active)) AND (all(n in nodes(p) WHERE (NOT exists(n.start_date) OR n.start_date <= {pdate}) AND (NOT exists(n.end_date) OR n.end_date > {pdate}) ))   AND (NOT exists(c.start_date) OR c.start_date <= {pdate}) AND (NOT exists(c.end_date) OR c.end_date > {pdate}) AND (NOT exists(c.active) OR c.active)  RETURN c"
    pars = {'pdate':get_today_num(), 'pnode_id': node_id}
    data = run_query(query, pars)
    return [dict (d['c'].properties.items() + {'capacity': get_contract_capacity(d['c'].properties['id'])}.items()) for d in data]


#check_no_overlapping_relationship("hpcreactores", "compute-1-1", "USES")


#print r


