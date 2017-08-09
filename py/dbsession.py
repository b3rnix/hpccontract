from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "otwauk"))

sess = driver.session()

def create_hpc_node(cluster, node_id, host_name, capacity):
    sess = driver.session()
    result = sess.run("MATCH (n:HPCNODE) WHERE n.id = {id} "
                           "RETURN count(*)",
                           {"id": node_id})

    if (result.peek()["count(*)"] > 0):
        raise Exception("Node already exists")


    sess.run(
            "CREATE (:HPCNODE{cluster:{pcluster}, id:{pid}, host_name:{phost_name}, capacity:{pcapacity}})",
            {
                "pcluster": cluster,
                "pid": node_id,
                "phost_name": host_name,
                "pcapacity": capacity
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

def segmentBroken(relationship):
    #TODO: Change this to the correct property analysis
    return False;
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

def get_available_capacity_for_contract(contract_id=None,uri=None):
    # Each contract contains one or more paths to the resources connected directly or indirectly to it.(i.e. a path connect a single
    # contract to a single resource)
    # Each path has a weight calculated as the product of the attribute "share" corresponding to
    # each segment forming the path. If the path is broken at one of its segments (i.e. the segment is inactive, dued, etc.),
    # then that segment's weight will be set to 0 and therefore the entire path's capacity will become 0.
    # The path's capacity represents how much capacity is drained from a node/resource to the linked contract via this path, and it is calculated
    # as the node's capacity times the path's capacity (which is always a number between 0 and 1).


    if (uri == None and contract_id == None):
        raise Exception("Must specify at least one contract identification (id or uri)")

    #MATCH (c:CONTRACT{id:"hpc_bancarizada"}) MATCH (n:HPCNODE)  RETURN (c)-[*..1]->(n);
    sess = driver.session()
    result = sess.run(
        "MATCH (c:CONTRACT{id:{pcontract_id},uri:{puri}}) MATCH (n:HPCNODE)  RETURN (c)-[*..6]->(n)",
        {
            "pcontract_id": contract_id,
            "uri": uri
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
print get_available_capacity_for_contract("hpc_bancarizada")

