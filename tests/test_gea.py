import momapy_kb.neo4j.core

import neo4j_dm.gea
import credentials

momapy_kb.neo4j.core.connect(
    credentials.URI, credentials.USERNAME, credentials.PASSWORD
)
gene_sets = neo4j_dm.gea.make_gene_sets_from_collection("COVID_DM_CD")
gmt_df = neo4j_dm.gea.make_gmt_df_from_gene_sets(gene_sets)
goat_df = neo4j_dm.gea.make_goat_analysis(
    gmt_df, "COVID_DM_CD", "./goat_gene_list.csv"
)
print(goat_df)
