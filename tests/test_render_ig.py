import os.path

import momapy.celldesigner.io.celldesigner
import momapy.io

import momapy_kb.neo4j.core

import neo4j_dm.ig
import neo4j_dm.queries

import credentials

momapy_kb.neo4j.core.connect(
    credentials.URI, credentials.USERNAME, credentials.PASSWORD
)
entry_id_to_map = {}
entry_id_to_ids = {}

for file_path in [
    "/home/rougny/research/commute/commute_dm_develop/build/data/pd_dm/celldesigner/Core_PD_map.xml"
]:
    file_name = os.path.basename(file_path)
    entry_id, _ = os.path.splitext(file_name)
    read_result = momapy.io.read(file_path)
    entry_id_to_map[entry_id] = read_result.obj
    entry_id_to_ids[entry_id] = read_result.ids
for i, (
    phenotype_node,
    nodes,
    relationships,
) in enumerate(
    neo4j_dm.queries.get_subgraphs_upstream_of_phenotype("autophagy")
):
    ig = neo4j_dm.ig.make_ig_from_nodes_and_relationships(nodes, relationships)
    neo4j_dm.ig.render_ig(
        ig, entry_id_to_map, entry_id_to_ids, f"test_{i}.svg"
    )
