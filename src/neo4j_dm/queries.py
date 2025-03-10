import typing

import momapy_kb.neo4j.core


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
    return result


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
    return result


def get_subgraph(
    node,
    relationship_types=None,
    mode: typing.Literal["all", "downstream", "upstream"] = "all",
    min_level=0,
    max_level=-1,
    filter_output_relationships=False,
):
    if relationship_types is None:
        relationship_types = []
    node_element_id = node.element_id
    if mode == "downstream":
        relationship_types = [
            f"{relationship_type}>" for relationship_type in relationship_types
        ]
    elif mode == "upstream":
        relationship_types = [
            f"<{relationship_type}" for relationship_type in relationship_types
        ]
    relationship_filter = "|".join(relationship_types)
    query = f"""
        MATCH (node)
        WHERE elementId(node) = "{node_element_id}"
        CALL apoc.path.subgraphAll(node, {{
            relationshipFilter: "{relationship_filter}",
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
