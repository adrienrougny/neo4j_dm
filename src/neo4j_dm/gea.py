import momapy_kb.neo4j.core

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
        file_name = neo4j_dm.utils.get_file_name_from_file_path(
            entry_node["file_path"]
        )
        gene_set = make_gene_set_from_nodes(nodes)
        gene_sets.append(
            (
                file_name,
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


def make_gmt_file_from_gene_sets(
    gene_sets: list[tuple[str, str, list[str]]], output_file_path
):
    gene_set_strings = []
    for gene_set in gene_sets:
        gene_set_string = (
            f"{gene_set[0]}\t{gene_set[1]}\t{'\t'.join(gene_set[2])}"
        )
        gene_set_strings.append(gene_set_string)
    with open(output_file_path, "w") as f:
        f.write("\n".join(gene_set_strings))
