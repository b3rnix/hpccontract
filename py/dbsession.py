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

assign_node_to_contract(node_id="compute-0-15", contract_id="hpc_bancarizada", share="100%", start_date=20170701)