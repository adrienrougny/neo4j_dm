import momapy_kb.neo4j.core

import neo4j_dm.gea
import credentials

momapy_kb.neo4j.core.connect(
    credentials.URI, credentials.USERNAME, credentials.PASSWORD
)

gene_sets = neo4j_dm.gea.make_gene_sets_from_collection("COVID_DM_CD")
neo4j_dm.gea.make_gmt_file_from_gene_sets(gene_sets, "test.gmt")
