import typing

import momapy_kb.neo4j.core

import neo4j_dm.ig


def get_ids(nodes):
    node_element_ids = [node.element_id for node in nodes]
    query = f"""
        MATCH
            (node:ModelElement)
        WHERE
            elementId(node) IN {node_element_ids}
        OPTIONAL MATCH
            (node)<-[:HAS_KEY]-(node_item:Item),
            (node_item)-[:HAS_VALUE]->(node_bag:Bag)-[:HAS_ELEMENT]->(node_id:String)
        RETURN node, collect(node_id.value)
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    return result


def get_annotations(nodes):
    node_element_ids = [node.element_id for node in nodes]
    query = f"""
        MATCH
            (node:ModelElement)
        WHERE
            elementId(node) IN {node_element_ids}
        OPTIONAL MATCH
            (node)<-[:HAS_KEY]-(node_item:Item),
            (node_item)-[:HAS_VALUE]->(node_bag:Bag)-[:HAS_ELEMENT]->(node_annotation:RDFAnnotation)
        RETURN node, collect(node_annotation)
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    return result


def get_ids_and_context(nodes):
    node_element_ids = [node.element_id for node in nodes]
    query = f"""
        MATCH
            (node:ModelElement)
        WHERE
            elementId(node) IN {node_element_ids}
        OPTIONAL MATCH
        (node)<-[:HAS_KEY]-(node_item:Item)<-[:HAS_ITEM]-(ids:Mapping)<-[:HAS_IDS]-(entry:CollectionEntry)<-[:HAS_ENTRY]-(collection:Collection),
            (node_item)-[:HAS_VALUE]->(node_bag:Bag)-[:HAS_ELEMENT]->(node_id:String)
        RETURN node, collect([node_id.value, entry, collection])
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    final_result = []
    for row in result:
        node = row[0]
        ids_and_context = [tuple(element) for element in row[1]]
        final_result.append(tuple([node, ids_and_context]))
    return final_result


def get_annotations_and_context(nodes):
    node_element_ids = [node.element_id for node in nodes]
    query = f"""
        MATCH
            (node:ModelElement)
        WHERE
            elementId(node) IN {node_element_ids}
        OPTIONAL MATCH
            (node)<-[:HAS_KEY]-(node_item:Item)<-[:HAS_ITEM]-(annotations:Mapping)<-[:HAS_RDF_ANNOTATIONS]-(entry:CollectionEntry)<-[:HAS_ENTRY]-(collection:Collection),
            (node_item)-[:HAS_VALUE]->(node_bag:Bag)-[:HAS_ELEMENT]->(node_annotation:RDFAnnotation)
        RETURN node, collect([node_annotation, entry, collection])
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    final_result = []
    for row in result:
        node = row[0]
        ids_and_context = [tuple(element) for element in row[1]]
        final_result.append(tuple([node, ids_and_context]))
    return final_result


def get_subgraph(
    node,
    relationship_types=None,
    exclude_labels=None,
    mode: typing.Literal["all", "downstream", "upstream"] = "all",
    min_level=0,
    max_level=-1,
    filter_output_relationships=False,
):
    if relationship_types is None:
        relationship_types = []
    if exclude_labels is None:
        exclude_labels = []
    node_element_id = node.element_id
    if mode == "downstream":
        relationship_types_for_filter = [
            f"{relationship_type}>" for relationship_type in relationship_types
        ]
    elif mode == "upstream":
        relationship_types_for_filter = [
            f"<{relationship_type}" for relationship_type in relationship_types
        ]
    relationship_filter = "|".join(relationship_types_for_filter)
    label_filter = "|".join([f"-{label}" for label in exclude_labels])
    query = f"""
        MATCH (node)
        WHERE elementId(node) = "{node_element_id}"
        CALL apoc.path.subgraphAll(node, {{
            relationshipFilter: "{relationship_filter}",
            labelFilter: "{label_filter}",
            minLevel: {min_level},
            maxLevel: {max_level}
        }})
        YIELD nodes, relationships
        RETURN nodes, relationships
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    nodes = result[0][0]
    relationships = result[0][1]
    if filter_output_relationships:
        relationships = [
            relationship
            for relationship in relationships
            if relationship.type in relationship_types
        ]
    return nodes, relationships


def get_subgraphs_upstream_of_phenotype(
    phenotype_name, filter_output_relationships=False
):
    upstream_subgraphs = []
    query = f"""
        MATCH (phenotype:Phenotype {{name: '{phenotype_name}'}})
        RETURN phenotype
    """
    result, _ = momapy_kb.neo4j.core.run(query)
    for row in result:
        phenotype_node = row[0]
        nodes, relationships = get_subgraph(
            phenotype_node,
            relationship_types=neo4j_dm.ig.INFLUENCES,
            mode="upstream",
            filter_output_relationships=filter_output_relationships,
        )
        upstream_subgraphs.append(
            (
                phenotype_node,
                nodes,
                relationships,
            )
        )
    return upstream_subgraphs
