import os
import tempfile

import boltons.iterutils
import pandas
import momapy_kb.neo4j.core
import rpy2.robjects.packages
import rpy2.robjects.vectors
import rpy2.robjects.pandas2ri


import neo4j_dm.utils
import neo4j_dm.queries


def make_gene_sets_from_collection(collection_name, check_connection=True):
    if check_connection:
        neo4j_dm.utils.check_connection()
    gene_sets = []
    query = f"""
        MATCH
            (collection:Collection)-[:HAS_ENTRY]->(entry:CollectionEntry),
            (entry)-[:HAS_RDF_ANNOTATIONS]->(annotations:Mapping),
            (annotations)-[:HAS_ITEM]->(item:Item),
            (item)-[:HAS_KEY]->(node)
        WHERE collection.name = '{collection_name}'
        RETURN entry, collect(node)
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    for row in result:
        entry_node = row[0]
        nodes = row[1]
        gene_set = make_gene_set_from_nodes(nodes)
        gene_sets.append(
            (
                entry_node["id_"],
                collection_name,
                gene_set,
            )
        )
    return gene_sets


def make_gene_set_from_nodes(nodes, namespace="ncbigene"):
    gene_set = set()
    for node, annotations in neo4j_dm.queries.get_annotations(nodes):
        for annotation in annotations:
            for resource in annotation["resources"]:
                if namespace in resource:
                    identifier = resource.split(":")[-1]
                    gene_set.add(identifier)
    return gene_set


def make_gmt_df_from_gene_sets(gene_sets: list[tuple[str, str, list[str]]]):
    # gene_sets: list of (gene_set_id, gene_set_desritption, list_of_gene_ids)
    gene_sets = [boltons.iterutils.flatten(gene_set) for gene_set in gene_sets]
    gmt_df = pandas.DataFrame(gene_sets)
    return gmt_df


def make_gmt_file_from_gene_sets(
    gene_sets: list[tuple[str, str, list[str]]],
    output_file_path,
):
    # gene_sets: list of (gene_set_id, gene_set_desritption, list_of_gene_ids)
    gmt_df = make_gmt_df_from_gene_sets(gene_sets)
    gmt_df.to_csv(output_file_path, sep="\t", header=False, index=False)


def make_goat_analysis(gmt_df_or_file, source, gene_list_path_file):

    def rpy2_df_to_pandas_df(rpy2_df):
        with (
            rpy2.robjects.default_converter + rpy2.robjects.pandas2ri.converter
        ).context():
            pandas_df = rpy2.robjects.conversion.get_conversion().rpy2py(
                rpy2_df
            )
        return pandas_df

    def pandas_df_to_rpy2_df(pandas_df):
        with (
            rpy2.robjects.default_converter + rpy2.robjects.pandas2ri.converter
        ).context():
            rpy2_df = rpy2.robjects.conversion.get_conversion().py2rpy(
                pandas_df
            )
        return rpy2_df

    r_packages = ["goat"]
    utils = rpy2.robjects.packages.importr("utils")
    utils.chooseCRANmirror(ind=1)
    r_packages_to_install = [
        r_package
        for r_package in r_packages
        if not rpy2.robjects.packages.isinstalled(r_package)
    ]
    utils.install_packages(
        rpy2.robjects.vectors.StrVector(r_packages_to_install)
    )
    goat = rpy2.robjects.packages.importr("goat")
    temp_file_path = None
    if isinstance(gmt_df_or_file, pandas.DataFrame):
        _, temp_file_path = tempfile.mkstemp()
        mode = "w"
        for index, row in gmt_df_or_file.iterrows():
            row = row.dropna()
            row = pandas.DataFrame(row).T
            row.to_csv(
                temp_file_path, sep="\t", mode=mode, index=False, header=False
            )
            mode = "a"
        gmt_df_or_file = temp_file_path
    r_gene_sets_df = goat.load_genesets_gmtfile(gmt_df_or_file, source)
    if temp_file_path is not None:
        os.remove(temp_file_path)
    r_gene_list_df = utils.read_csv(gene_list_path_file)
    pandas_gene_list_df = rpy2_df_to_pandas_df(r_gene_list_df)
    pandas_gene_list_df["signif"] = pandas_gene_list_df["signif"].map(
        {"True": True, "False": False}
    )
    pandas_gene_list_df = pandas_gene_list_df.head(20000)
    r_gene_list_df = pandas_df_to_rpy2_df(pandas_gene_list_df)
    r_gene_sets_filtered_df = goat.filter_genesets(
        r_gene_sets_df, r_gene_list_df, min_overlap=10, max_overlap=1500
    )
    r_result_df = goat.test_genesets(
        r_gene_sets_filtered_df,
        r_gene_list_df,
        method="goat",
        score_type="effectsize",
        padj_method="fdr",
        padj_cutoff=0.05,
    )
    _, temp_file_path = tempfile.mkstemp(suffix=".csv")
    goat.save_genesets(r_result_df, r_gene_list_df, filename=temp_file_path)
    goat_df = pandas.read_csv(temp_file_path)
    os.remove(temp_file_path)
    return goat_df
