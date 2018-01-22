from api import *
#generate_slurm_credits_command("hpc_bancarizada", 30)
# create_hpc_contract(id="quimica_bancarizada",description="Acceso a Bancarizada Gerencia Quimica",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:quimicabancarizada", active=True )
# create_hpc_contract(id="fisica_bancarizada",description="Acceso a Bancarizada Gerencia Fisica",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:fisicabancarizada", active=True )
# create_hpc_contract(id="gtic_bancarizada",description="Acceso a Bancarizada Gerencia GTIC",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:gticbancarizada", active=True )
# create_hpc_contract(id="gpuresearch",description="Investigacion GPU",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:gticgpuresearch", active=True )
#
# assign_sub_contract("quimica_bancarizada", "hpc_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)
# assign_sub_contract("fisica_bancarizada", "hpc_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)
# assign_sub_contract("gtic_bancarizada", "hpc_bancarizada", share="20%", start_date=20170701, end_date=20180801, active=True)
# assign_sub_contract("gpuresearch", "gtic_bancarizada", share="30%", start_date=20170701, end_date=20180801, active=True)
#
# create_entity("GTIC", "G.T.I.C.", "GERENCIA")
# create_entity("FISICA", "Gerencia de Fisica", "GERENCIA")
# create_entity("QUIMICA", "Gerencia de Quimica", "GERENCIA")
#
# create_entity("lanieto@cnea.gov.ar", "Nieto", "PERSON")
#
# assign_contract_administrator(contract_id="quimica_bancarizada", person_id="bernabepanarello@cnea.gov.ar", start_date=20170801, end_date=20190801)
# assign_contract_administrator(contract_id="gtic_bancarizada", person_id="rgarcia@cnea.gov.ar", start_date=20170801, end_date=20190801)
# assign_contract_administrator(contract_id="gpuresearch", person_id="bernabepanarello@cnea.gov.ar", start_date=20170801, end_date=20190801)
# assign_contract_administrator(contract_id="hpc_bancarizada", person_id="iozzo@cnea.gov.ar", start_date=20170801, end_date=20190731)
# assign_contract_administrator(contract_id="hpc_bancarizada", person_id="lanieto@cnea.gov.ar", start_date=20190801, end_date=20220731)
#
# assign_contract_owner(contract_id="hpc_bancarizada", entity_id="GTIC", entity_type="GERENCIA" )
#
# create_hpc_node("neurus", "compute-1-0", "compute-1-0", 24)
# create_hpc_node("neurus", "compute-1-1", "compute-1-1", 24)
# create_hpc_node("neurus", "compute-1-2", "compute-1-2", 24)
#
# create_hpc_contract(id="hpcreactores",description="Proyecto Reactores",start_date=20170701, uri="slurm://c:neurus/p:reactores/a:reactores", active=True )
# assign_node_to_contract(node_id="compute-1-0", contract_id="hpcreactores", share="80%", start_date=20170701)
# assign_node_to_contract(node_id="compute-1-1", contract_id="hpcreactores", share="80%", start_date=20170701)
# assign_node_to_contract(node_id="compute-1-2", contract_id="hpcreactores", share="80%", start_date=20170701)
#
# assign_node_to_contract(node_id="compute-1-0", contract_id="hpc_bancarizada", share="20%", start_date=20170701)
# assign_node_to_contract(node_id="compute-1-1", contract_id="hpc_bancarizada", share="20%", start_date=20170701)
# assign_node_to_contract(node_id="compute-1-2", contract_id="hpc_bancarizada", share="20%", start_date=20170701)
#
# create_entity("rios@cnea.gov.ar", "Rios", "PERSON")
# assign_contract_administrator(contract_id="hpcreactores", person_id="rios@cnea.gov.ar", start_date=20170801, end_date=20190801)

#generate_slurm_credits_command("hpcreactores", 30)

#create_hpc_node("neurus", "compute-2-15", "compute-2-15", 24)
#assign_node_to_contract(node_id="compute-2-15", contract_id="gpuresearch", share="100%", start_date=20170701)


#create_hpc_contract(id="leecher",description="Leecher de bancarizada",start_date=20170701,uri="slurm://c:neurus/p:bancarizada/a:leecher", active=True )
#link_contracts_by_use("hpcreactores", "leecher", share="50%", start_date=20170701, end_date=20190101, active=True)

# VOLUMENES: MATCH (c:CONTRACT:NAS)-[*2..2]->(n:NAS:NODE) RETURN c


#assign_contract_user("leecher", "bernabepanarello", start_date=None, end_date=20180229, active=True)
#users = get_slurm_partition_users('bancarizada')
#create_nas_node("neurus", "nas-0-0", "nas-0-0", 50000)

#create_nas_contract("nas_neurus_gerencias", "Contrato NAS para Gerencias Cluster Neurus","",20180101)
#assign_nas_node_to_contract("nas-0-0", "nas_neurus_gerencias")
#create_nas_contract("nas_neurus_gerencias_gerencia1", "Contrato NAS para Gerencia 1 Cluster Neurus","",20180101)
#create_nas_contract("nas_neurus_gerencias_gerencia2", "Contrato NAS para Gerencia 2 Cluster Neurus","",20180101)
#create_nas_contract("nas_neurus_gerencias_gerencia3", "Contrato NAS para Gerencia 3 Cluster Neurus","",20180101)
#link_contracts_by_use("nas_neurus_gerencias","nas_neurus_gerencias_gerencia1","10000")
#link_contracts_by_use("nas_neurus_gerencias","nas_neurus_gerencias_gerencia2","10000")
#link_contracts_by_use("nas_neurus_gerencias","nas_neurus_gerencias_gerencia3","5000")
#create_nas_contract("nas_neurus_gerencia1_grupoa", "Gerencia 1 Geupo A","zfs://nas-0-0/volg1/ga",20180101)
#create_nas_contract("nas_neurus_gerencia1_grupob", "Gerencia 1 Geupo B","zfs://nas-0-0/volg1/gb",20180101)
#create_nas_contract("nas_neurus_gerencia1_grupoc", "Gerencia 1 Geupo C","zfs://nas-0-0/volg1/gc",20180101)
#link_contracts_by_use("nas_neurus_gerencias_gerencia1","nas_neurus_gerencia1_grupoa","100")
#link_contracts_by_use("nas_neurus_gerencias_gerencia1","nas_neurus_gerencia1_grupob","100")
#link_contracts_by_use("nas_neurus_gerencias_gerencia1","nas_neurus_gerencia1_grupoc","100")
#assign_contract_user("nas_neurus_gerencia1_grupoa", "gaston")
#create_nas_contract("nas_neurus_gerencias_gerencia4", "Contrato NAS para Gerencia 4 Cluster Neurus","",20180101)
#link_contracts_by_use("nas_neurus_gerencias","nas_neurus_gerencias_gerencia4","25%")
#r=get_nas_volume_contracts("neurus", "nas-0-0")

#check_no_overlapping_relationship("hpcreactores", "compute-1-1", "USES")
#get_relationships("bernabepanarello","leecher")

#from mako.template import Template
#mytemplate = Template(filename='./templates/assign_slurm_credits.txt')
#print(mytemplate.render( cluster='neurus', account='reactores', credits =80, partition='reactores'))
#data = get_credits_for_all_hpc_contracts()
#pass


from mako.template import Template
mytemplate = Template(filename='./templates/assign_slurm_credits_all_contracts.txt')
data = get_credits_for_all_hpc_contracts()
print(mytemplate.render( cluster='neurus', contracts=data))

pass