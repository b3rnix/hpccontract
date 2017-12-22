import datetime
from neo4j.v1 import GraphDatabase, basic_auth
import re


driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "otwauk"))

sess = driver.session()

slurmuriregex  = re.compile('slurm:\/\/c:([a-zA-Z0-9]+)\/p:([a-zA-Z0-9]+)\/a:([a-zA-Z0-9]+)')
def get_slurm_data_from_uri(uri):
    m = slurmuriregex.match(uri)
    if m == None:
        raise Exception("Invalid SLURM URI!!")

    return {
        "cluster": m.groups()[0],
        "partition": m.groups()[1],
        "account": m.groups()[2]
    }

def get_contract(contract_id):
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
    sess = driver.session()
    result = sess.run("MATCH (n:HPCNODE) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": node_id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Node already exists")


    sess.run(
            "CREATE (:HPCNODE{cluster:{pcluster}, id:{pid}, host_name:{phost_name}, capacity:{pcapacity},uri:{puri}})",
            {
                "pcluster": cluster,
                "pid": node_id,
                "phost_name": host_name,
                "pcapacity": capacity,
                "puri": "slurm://c:{0}/n:{1}".format(cluster,node_id)
             }
             )

    sess.close()

def create_nas_node(cluster, node_id, host_name, capacity):
    sess = driver.session()
    result = sess.run("MATCH (n:NASNODE) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": node_id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Node already exists")


    sess.run(
            "CREATE (:NASNODE{cluster:{pcluster}, id:{pid}, host_name:{phost_name}, capacity:{pcapacity},uri:{puri}})",
            {
                "pcluster": cluster,
                "pid": node_id,
                "phost_name": host_name,
                "pcapacity": capacity,
                "puri": "nas://c:{0}/n:{1}".format(cluster,node_id)
             }
             )

    sess.close()


def create_nas_contract(id, description, uri, start_date=None, end_date=None, active=True):
    sess = driver.session()
    result = sess.run("MATCH (n:CONTRACT:NAS) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Contract already exists")


    sess.run(
            "CREATE (:CONTRACT:NAS{id:{pid}, description:{pdescription}, start_date:{pstart_date}, end_date:{pend_date}, uri:{puri}, active:{pactive}})",
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




def create_entity(entity_id, entity_name, entity_type):
    sess = driver.session()
    result = sess.run("MATCH (n) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": entity_id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Entity already exists")


    sess.run(
            "CREATE (:" + entity_type + "{id:{pid}, name:{pname}})",
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
            "CREATE (:CONTRACT:HPC{id:{pid}, description:{pdescription}, start_date:{pstart_date}, end_date:{pend_date}, uri:{puri}, active:{pactive}})",
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

def assign_node_to_contract(node_id, contract_id, share=None, start_date=None, end_date=None, active=True):
    sess = driver.session()
    sess.run(
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (n:HPCNODE) WHERE n.id = {pnode_id} CREATE (c)-[:USES{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(n)",
        {
            "pnode_id": node_id,
            "pcontract_id": contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )

    sess.close()


def assign_sub_contract(child_contract_id, parent_contract_id, share=None, start_date=None, end_date=None, active=True):
    sess = driver.session()
    sess.run(
        "MATCH (p:CONTRACT) WHERE p.id = {pparentcontract_id} MATCH (c:CONTRACT) WHERE c.id = {pchildcontract_id} CREATE (c)-[:SUBCONTRACT{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(p)",
        {
            "pparentcontract_id": parent_contract_id,
            "pchildcontract_id": child_contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )

    sess.close()


def assign_contract_administrator(contract_id, person_id, start_date=None, end_date=None, active=True):
    sess = driver.session()
    sess.run(
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:PERSON) WHERE p.id = {pperson_id} CREATE (p)-[:ADMINISTRATIR{start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(c)",
        {
            "pcontract_id": contract_id,
            "pperson_id": person_id,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )

    sess.close()


def assign_contract_owner(contract_id, entity_id, entity_type):
    sess = driver.session()
    sess.run(
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:" + entity_type + ") WHERE p.id = {pentity_id} CREATE (p)-[:OWNER]->(c)",
        {
            "pcontract_id": contract_id,
            "pentity_id": entity_id

        }
    )

    sess.close()


def assign_contract_user(contract_id, entity_id, start_date=None, end_date=None, active=True):
    sess = driver.session()
    sess.run(
        "MATCH (c:CONTRACT) WHERE c.id = {pcontract_id} MATCH (p:" + "USER" + ") WHERE p.id = {pentity_id} CREATE (p)-[:USES{start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(c)",
        {
            "pcontract_id": contract_id,
            "pentity_id": entity_id,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active


        }
    )

    sess.close()



#Creates a USES relationship so origin_contract gives resources to dst_contract.   (destination_contract)-[USES]->(origin_contract)
def link_contracts_by_use(origin_contract_id, destination_contract_id, share=None, start_date=None, end_date=None, active=True):
    get_contract(origin_contract_id)
    get_contract(destination_contract_id)
    sess = driver.session()
    sess.run(
        "MATCH (p:CONTRACT) WHERE p.contract_id = {porigincontract_id} MATCH (c:CONTRACT) WHERE c.contract_id = {pdestinationcontract_id} CREATE (c)-[:USES{share:{pshare},start_date:{pstart_date},end_date:{pend_date}, active:{pactive}}]->(p)",
        {
            "porigincontract_id": origin_contract_id,
            "pdestinationcontract_id": destination_contract_id,
            "pshare": share,
            "pstart_date": start_date,
            "pend_date": end_date,
            "pactive": active

        }
    )

    sess.close()



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
    sess = driver.session()

    set_keys = map(lambda k: "SET n." + k + "={p" + k + "}" if not kwargs[k] is None else "", kwargs.keys())
    set_values = {}
    set_values["id"] = id
    for k in kwargs:
        if not kwargs[k] is None:
            set_values["p"+k] = kwargs[k]

    sess.run(
            "MATCH (n) WHERE n.id = {id} " + " ".join(set_keys), set_values)

    sess.close()


def segmentBroken(r):
    return not isActive(r)

def isActive(n):
    current_date = datetime.date.year * 10000 + datetime.date.month * 100 + datetime.date.day
    active = (not hasattr(n, 'start_date') or n.start_date >= current_date) and (not hasattr(n, 'end_date') or n.end_date <= current_date)
    active = active and (not hasattr(n, 'active') or n.active)
    return active

def extractNormalizedShareFromRelationShips(relationship):
    if (segmentBroken(relationship)):
        return 0
    if ("share" in relationship.properties):
        return float(relationship.properties["share"].strip ("%")) / 100
    else:
        return 1

def calculatePathWeight(path):
    relationships = path.relationships
    shares = map (extractNormalizedShareFromRelationShips, relationships)
    factor = reduce(lambda fac,share: fac * share, shares,1.0)
    return factor

def get_available_capacity_for_contract(contract_id=None):
    # Each contract contains one or more paths to the resources connected directly or indirectly to it.(i.e. a path connect a single
    # contract to a single resource)
    # Each path has a weight calculated as the product of the attribute "share" corresponding to
    # each segment forming the path. If the path is broken at one of its segments (i.e. the segment is inactive, dued, etc.),
    # then that segment's weight will be set to 0 and therefore the entire path's capacity will become 0.
    # The path's capacity represents how much capacity is drained from a node/resource to the linked contract via this path, and it is calculated
    # as the node's capacity times the path's capacity (which is always a number between 0 and 1).


    #MATCH (c:CONTRACT{id:"hpc_bancarizada"}) MATCH (n:HPCNODE)  RETURN (c)-[*..1]->(n);
    sess = driver.session()
    result = sess.run(
        "MATCH (c:CONTRACT{id:{pcontract_id}}) MATCH (n:HPCNODE)  RETURN (c)-[*]->(n)",
        {
            "pcontract_id": contract_id

        }
    )
    data = result.data()
    sess.close()
    #for pathentry in data[0].values():

    contractcapacity = 0.0
    for pathentry in data:
        if not (pathentry.values() == [[]]) > 0:
            path= pathentry.values()[0][0]
            weight = calculatePathWeight(path)
            resource = path.end
            capacity = resource.properties["capacity"]
            pathcapacity = capacity * weight
            contractcapacity+=pathcapacity

    return contractcapacity

#assign_node_to_contract(node_id="compute-0-8", contract_id="hpc_bancarizada", share="50%", start_date=20170701)
#print get_available_capacity_for_contract("hpc_bancarizada")


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

#generate_slurm_credits_command("hpc_bancarizada", 30)



#create_hpc_contract(id="quimica_bancarizada",description="Acceso a Bancarizada Gerencia Quimica",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:quimicabancarizada", active=True )
#create_hpc_contract(id="fisica_bancarizada",description="Acceso a Bancarizada Gerencia Fisica",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:fisicabancarizada", active=True )
#create_hpc_contract(id="gtic_bancarizada",description="Acceso a Bancarizada Gerencia GTIC",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:gticbancarizada", active=True )
#create_hpc_contract(id="gpuresearch",description="Investigacion GPU",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:gticgpuresearch", active=True )

#assign_sub_contract("quimica_bancarizada", "hpc_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)
#assign_sub_contract("fisica_bancarizada", "hpc_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)
#assign_sub_contract("gtic_bancarizada", "hpc_bancarizada", share="20%", start_date=20170701, end_date=20180801, active=True)
#assign_sub_contract("gpuresearch", "gtic_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)

#create_entity("GTIC", "G.T.I.C.", "GERENCIA")
#create_entity("FISICA", "Gerencia de Fisica", "GERENCIA")
#create_entity("QUIMICA", "Gerencia de Quimica", "GERENCIA")

#create_entity("lanieto@cnea.gov.ar", "Nieto", "PERSON")

#assign_contract_administrator(contract_id="quimica_bancarizada", person_id="bernabepanarello@cnea.gov.ar", start_date=20170801, end_date=20190801)
#assign_contract_administrator(contract_id="gtic_bancarizada", person_id="rgarcia@cnea.gov.ar", start_date=20170801, end_date=20190801)
#assign_contract_administrator(contract_id="gpuresearch", person_id="bernabepanarello@cnea.gov.ar", start_date=20170801, end_date=20190801)
#assign_contract_administrator(contract_id="hpc_bancarizada", person_id="iozzo@cnea.gov.ar", start_date=20170801, end_date=20190731)
#assign_contract_administrator(contract_id="hpc_bancarizada", person_id="lanieto@cnea.gov.ar", start_date=20190801, end_date=20220731)

#assign_contract_owner(contract_id="hpc_bancarizada", entity_id="GTIC", entity_type="GERENCIA" )

#create_hpc_node("neurus", "compute-1-0", "compute-1-0", 24)
#create_hpc_node("neurus", "compute-1-1", "compute-1-1", 24)
#create_hpc_node("neurus", "compute-1-2", "compute-1-2", 24)

#create_hpc_contract(id="hpcreactores",description="Proyecto Reactores",start_date=20170701, uri="slurm://c:neurus/p:reactores/a:reactores", active=True )
#assign_node_to_contract(node_id="compute-1-0", contract_id="hpcreactores", share="80%", start_date=20170701)
#assign_node_to_contract(node_id="compute-1-1", contract_id="hpcreactores", share="80%", start_date=20170701)
#assign_node_to_contract(node_id="compute-1-2", contract_id="hpcreactores", share="80%", start_date=20170701)

#assign_node_to_contract(node_id="compute-1-0", contract_id="hpc_bancarizada", share="20%", start_date=20170701)
#assign_node_to_contract(node_id="compute-1-1", contract_id="hpc_bancarizada", share="20%", start_date=20170701)
#assign_node_to_contract(node_id="compute-1-2", contract_id="hpc_bancarizada", share="20%", start_date=20170701)

#create_entity("rios@cnea.gov.ar", "Rios", "PERSON")
#assign_contract_administrator(contract_id="hpcreactores", person_id="rios@cnea.gov.ar", start_date=20170801, end_date=20190801)

#generate_slurm_credits_command("hpcreactores", 30)

#create_hpc_node("neurus", "compute-2-15", "compute-2-15", 24)
#assign_node_to_contract(node_id="compute-2-15", contract_id="gpuresearch", share="100%", start_date=20170701)


update_contract("hpc_bancarizada", start_date= 20180101)