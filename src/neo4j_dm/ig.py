import copy

import pygraphviz
import momapy.coloring
import momapy.celldesigner.core
import momapy.rendering.svg_native
import momapy.rendering.core
import momapy_kb.neo4j.core

import neo4j_dm.utils
import neo4j_dm.queries

REACTANT_TO_PRODUCT = "_REACTANT_TO_PRODUCT"
POSITIVE_INFLUENCE = "_POSITIVE_INFLUENCE"
NEGATIVE_INFLUENCE = "_NEGATIVE_INFLUENCE"
NECESSARY_POSITIVE_INFLUENCE = "_NECESSARY_POSITIVE_INFLUENCE"
UNCERTAIN_POSITIVE_INFLUENCE = "_UNCERTAIN_POSITIVE_INFLUENCE"
UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE = (
    "_UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE"
)
UNCERTAIN_NEGATIVE_INFLUENCE = "_UNCERTAIN_NEGATIVE_INFLUENCE"

INFLUENCES = [
    REACTANT_TO_PRODUCT,
    POSITIVE_INFLUENCE,
    NEGATIVE_INFLUENCE,
    NECESSARY_POSITIVE_INFLUENCE,
]
POINTS_PER_INCH = 96


class InfluenceGraph(dict):

    def get_nodes(self):
        return set(self.keys())

    def add_node(self, node):
        self[node] = set()

    def add_relationship(self, relationship):
        self[relationship.end_node].add(relationship)

    def get_relationships(self):
        relationships = set([])
        for node in self.get_nodes():
            relationships.update(self[node])
        return relationships

    def get_stimulators(self, node):
        return [
            relationship.start_node
            for relationship in self[node]
            if relationship.type == POSITIVE_INFLUENCE
        ]

    def get_inhibitors(self, node):
        return [
            relationship.start_node
            for relationship in self[node]
            if relationship.type == NEGATIVE_INFLUENCE
        ]

    def get_necessary_stimulators(self, node):
        return [
            relationship.start_node
            for relationship in self[node]
            if relationship.type == NECESSARY_POSITIVE_INFLUENCE
            or relationship.type == REACTANT_TO_PRODUCT
        ]

    def get_modulators(self, node):
        return (
            self.get_stimulators(node)
            + self.get_inhibitors(node)
            + self.get_necessary_stimulators(node)
        )

    def remove_node(self, node):
        del self[node]
        for other_node in self.get_nodes():
            for relationship in list(self[other_node]):
                if relationship.start_node == node:
                    self[other_node].remove(relationship)


def make_ig_in_db(check_connection=True):
    if check_connection:
        neo4j_dm.utils.check_connection()
    queries = []
    queries.append(
        f"""
        MATCH
            (reaction:Reaction),
            (reaction)-[:HAS_REACTANT]->(reactant:Reactant),
            (reactant)-[:HAS_REFERRED_SPECIES]->(reactant_species:Species),
            (reaction)-[:HAS_PRODUCT]->(product:Product),
            (product)-[:HAS_REFERRED_SPECIES]->(product_species:Species)
        MERGE
            (reactant_species)-[r:{REACTANT_TO_PRODUCT}]->(product_species)
        WITH
            r AS r,
            reaction AS reaction
        MATCH (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_REACTION]->(reaction),
            (entry)-[:HAS_IDS]->(ids:Mapping)-[:HAS_ITEM]->(reaction_item:Item)-[:HAS_KEY]->(reaction),
            (reaction_item)-[:HAS_VALUE]->(reaction_bag:Bag)-[:HAS_ELEMENT]->(reaction_id:String),
            (ids)-[:HAS_ITEM]->(model_item:Item)-[:HAS_KEY]->(model),
            (model_item)-[:HAS_VALUE]->(model_bag:Bag)-[:HAS_ELEMENT]->(model_id:String)
        WITH
            r AS r,
            collect(model_id.value) AS model_ids,
            collect(reaction_id.value) AS reaction_ids
        SET
            r.reaction_ids = reaction_ids,
            r.model_ids = model_ids
        RETURN
            r
        """
    )
    for labels, relationship_type in [
        (["PhysicalStimulator", "Catalyzer"], POSITIVE_INFLUENCE),
        (["Trigger"], NECESSARY_POSITIVE_INFLUENCE),
        (["Inhibitor"], NEGATIVE_INFLUENCE),
        (
            [
                "UnknownPhysicalStimulator",
                "UnknownCatalyzer",
            ],
            UNCERTAIN_POSITIVE_INFLUENCE,
        ),
        (["UnknownInhibitor"], UNCERTAIN_NEGATIVE_INFLUENCE),
        (["UnknownTrigger"], UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE),
    ]:
        for label in labels:
            queries.append(
                f"""
                MATCH
                    (reaction:Reaction),
                    (reaction)-[:HAS_MODIFIER]->(modulator:{label}),
                    (modulator)-[:HAS_REFERRED_SPECIES]->(modulator_species:Species),
                    (reaction)-[:HAS_PRODUCT]->(product:Product),
                    (product)-[:HAS_REFERRED_SPECIES]->(product_species:Species)
                MERGE
                    (modulator_species)-[r:{relationship_type}]->(product_species)
                WITH
                    r AS r,
                    reaction AS reaction
                MATCH
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_REACTION]->(reaction),
                    (entry)-[:HAS_IDS]->(ids:Mapping)-[:HAS_ITEM]->(reaction_item:Item)-[:HAS_KEY]->(reaction),
                    (reaction_item)-[:HAS_VALUE]->(reaction_bag:Bag)-[:HAS_ELEMENT]->(reaction_id:String),
                    (ids)-[:HAS_ITEM]->(model_item:Item)-[:HAS_KEY]->(model),
                    (model_item)-[:HAS_VALUE]->(model_bag:Bag)-[:HAS_ELEMENT]->(model_id:String)
                WITH
                    r AS r,
                    collect(model_id.value) AS model_ids,
                    collect(reaction_id.value) AS reaction_ids
                SET
                    r.reaction_ids = reaction_ids,
                    r.model_ids = model_ids
                RETURN
                    r
                """
            )
            queries.append(
                f"""
                MATCH
                    (reaction:Reaction),
                    (reaction)-[:HAS_MODIFIER]->(modulator:{label}),
                    (modulator)-[:HAS_REFERRED_SPECIES]->(modulator_gate:BooleanLogicGate),
                    (modulator_gate)-[:HAS_INPUT]->(modulator_species:Species),
                    (reaction)-[:HAS_PRODUCT]->(product:Product),
                    (product)-[:HAS_REFERRED_SPECIES]->(product_species:Species)
                WHERE
                    NOT modulator_gate:NotGate
                MERGE
                    (modulator_species)-[r:{relationship_type}]->(product_species)
                WITH
                    r AS r,
                    reaction AS reaction
                MATCH
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_REACTION]->(reaction),
                    (entry)-[:HAS_IDS]->(ids:Mapping)-[:HAS_ITEM]->(reaction_item:Item)-[:HAS_KEY]->(reaction),
                    (reaction_item)-[:HAS_VALUE]->(reaction_bag:Bag)-[:HAS_ELEMENT]->(reaction_id:String),
                    (ids)-[:HAS_ITEM]->(model_item:Item)-[:HAS_KEY]->(model),
                    (model_item)-[:HAS_VALUE]->(model_bag:Bag)-[:HAS_ELEMENT]->(model_id:String)
                WITH
                    r AS r,
                    collect(model_id.value) AS model_ids,
                    collect(reaction_id.value) AS reaction_ids
                SET
                    r.reaction_ids = reaction_ids,
                    r.model_ids = model_ids
                RETURN
                    r
                """
            )
    for labels, relationship_type in [
        (
            ["Catalysis", "PhysicalStimulation", "PositiveInfluence"],
            POSITIVE_INFLUENCE,
        ),
        (["Triggering"], NECESSARY_POSITIVE_INFLUENCE),
        (["Inhibition", "NegativeInfluence"], NEGATIVE_INFLUENCE),
        (
            [
                "UnknownCatalysis",
                "UnknownPhysicalStimulation",
                "UnknownPositiveInfluence",
            ],
            UNCERTAIN_POSITIVE_INFLUENCE,
        ),
        (["UnknownTriggering"], UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE),
        (
            ["UnknownInhibition", "UnknownNegativeInfluence"],
            UNCERTAIN_NEGATIVE_INFLUENCE,
        ),
    ]:
        for label in labels:
            queries.append(
                f"""
                MATCH
                    (modulation:{label}),
                    (modulation)-[:HAS_SOURCE]->(source_species:Species),
                    (modulation)-[:HAS_TARGET]->(target_species:Species)
                MERGE
                    (source_species)-[r:{relationship_type}]->(target_species)
                WITH
                    r AS r,
                    modulation AS modulation
                MATCH
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_MODULATION]->(modulation),
                    (entry)-[:HAS_IDS]->(ids:Mapping)-[:HAS_ITEM]->(reaction_item:Item)-[:HAS_KEY]->(modulation),
                    (reaction_item)-[:HAS_VALUE]->(reaction_bag:Bag)-[:HAS_ELEMENT]->(reaction_id:String),
                    (ids)-[:HAS_ITEM]->(model_item:Item)-[:HAS_KEY]->(model),
                    (model_item)-[:HAS_VALUE]->(model_bag:Bag)-[:HAS_ELEMENT]->(model_id:String)
                WITH
                    r AS r,
                    collect(model_id.value) AS model_ids,
                    collect(reaction_id.value) AS reaction_ids
                SET
                    r.reaction_ids = reaction_ids,
                    r.model_ids = model_ids
                RETURN
                    r
                """
            )
            queries.append(
                f"""
                MATCH
                    (modulation:{label}),
                    (modulation)-[:HAS_SOURCE]->(source:BooleanLogicGate),
                    (source)-[:HAS_INPUT]->(source_species:Species),
                    (modulation)-[:HAS_TARGET]->(target_species:Species)
                WHERE
                    NOT source:NotGate
                MERGE
                    (source_species)-[r:{relationship_type}]->(target_species)
                WITH
                    r AS r,
                    modulation AS modulation
                MATCH
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_MODULATION]->(modulation),
                    (entry)-[:HAS_IDS]->(ids:Mapping)-[:HAS_ITEM]->(reaction_item:Item)-[:HAS_KEY]->(modulation),
                    (reaction_item)-[:HAS_VALUE]->(reaction_bag:Bag)-[:HAS_ELEMENT]->(reaction_id:String),
                    (ids)-[:HAS_ITEM]->(model_item:Item)-[:HAS_KEY]->(model),
                    (model_item)-[:HAS_VALUE]->(model_bag:Bag)-[:HAS_ELEMENT]->(model_id:String)
                WITH
                    r AS r,
                    collect(model_id.value) AS model_ids,
                    collect(reaction_id.value) AS reaction_ids
                SET
                    r.reaction_ids = reaction_ids,
                    r.model_ids = model_ids
                RETURN
                    r
                """
            )
    for query in queries:
        _ = momapy_kb.neo4j.core.run(query)


def make_ig_from_nodes_and_relationships(
    nodes, relationships, include=None, max_n_nodes=-1
):
    ig = InfluenceGraph()
    if include is None:
        include = nodes
    for node in include[:max_n_nodes]:
        node._graph = None
        ig.add_node(node)
    for relationship in relationships:
        relationship.start_node._graph = None
        relationship.end_node._graph = None
        end_node = relationship.end_node
        if end_node in ig.get_nodes():
            ig.add_relationship(relationship)
    for node in ig.get_nodes():
        for modulator in ig.get_modulators(node):
            if modulator not in ig.get_nodes():
                ig.add_node(modulator)
    return ig


def prune_trivial_nodes(ig, recursive=True):
    ig = copy.deepcopy(ig)
    deleted = True
    while deleted:
        deleted = False
        for node in ig.get_nodes():
            if not ig.get_modulators(node):
                ig.remove_node(node)
                deleted = True
        if not recursive:
            break
    return ig


def get_number_and_size_of_components(ig):
    list_of_sets = []
    for node in ig.get_nodes():
        for modulator in ig.get_modulators(node):
            list_of_sets.append(set([node, modulator]))
    return neo4j_dm.utils.get_number_and_size_of_clusters(list_of_sets)


def render_ig(
    ig, entry_id_to_map, entry_id_to_ids, output_file_path, color_node_ids=None
):

    def translate_layout_element(layout_element, tx, ty):
        layout_element.position = layout_element.position + (tx, ty)
        for sub_layout_element in layout_element.children():
            translate_layout_element(sub_layout_element, tx, ty)

    if color_node_ids is None:
        color_node_ids = []

    relationship_type_to_cd_class = {
        "_POSITIVE_INFLUENCE": momapy.celldesigner.core.PositiveInfluenceLayout,
        "_NECESSARY_POSITIVE_INFLUENCE": momapy.celldesigner.core.TriggeringLayout,
        "_REACTANT_TO_PRODUCT": momapy.celldesigner.core.TriggeringLayout,
        "_NEGATIVE_INFLUENCE": momapy.celldesigner.core.InhibitionLayout,
    }
    node_to_layout_element = {}
    for node in ig.get_nodes():
        node_ids_and_context = neo4j_dm.queries.get_ids_and_context([node])
        for _, ids_and_context in node_ids_and_context:
            for id_and_context in ids_and_context:
                node_id = id_and_context[0]
                entry_node = id_and_context[1]
                break
            break
        entry_id = entry_node["id_"]
        map_ = entry_id_to_map[entry_id]
        ids = entry_id_to_ids[entry_id]
        model_element = None
        for candidate_model_element in ids:
            if node_id in ids[candidate_model_element]:
                model_element = candidate_model_element
                break
        layout_element = map_.layout_model_mapping.get_mapping(
            model_element, unpack=True
        )
        node_to_layout_element[node] = layout_element
    map_builder = momapy.celldesigner.core.CellDesignerMapBuilder()
    layout_builder = map_builder.new_layout()
    map_builder.layout = layout_builder
    a_graph = pygraphviz.AGraph()
    node_id_to_coordinates = {}
    for node in ig.get_nodes():
        a_graph.add_node(node["id_"])
        for modulator in ig.get_modulators(node):
            a_graph.add_edge(modulator["id_"], node["id_"])
    for node, layout_element in node_to_layout_element.items():
        a_node = a_graph.get_node(node["id_"])
        a_node.attr["width"] = layout_element.width / POINTS_PER_INCH
        a_node.attr["height"] = layout_element.height / POINTS_PER_INCH
    a_graph.graph_attr["ranksep"] = 1.0
    a_graph.layout(prog="dot")
    for a_node in a_graph.nodes():
        x, y = [float(coord) for coord in a_node.attr["pos"].split(",")]
        node_id_to_coordinates[str(a_node)] = (
            x,
            y,
        )
    node_id_to_layout_element_moved = {}
    for node, layout_element in node_to_layout_element.items():
        layout_element_builder = momapy.builder.builder_from_object(
            layout_element
        )
        coordinates = node_id_to_coordinates[node["id_"]]
        old_position = layout_element_builder.position
        new_position = momapy.geometry.Point.from_tuple(coordinates)
        tx, ty = new_position - old_position
        translate_layout_element(layout_element_builder, tx, ty)
        layout_builder.layout_elements.append(layout_element_builder)
        node_id_to_layout_element_moved[node["id_"]] = layout_element_builder
    for relationship in ig.get_relationships():
        start_node = relationship.start_node
        end_node = relationship.end_node
        start_layout_element = node_id_to_layout_element_moved[
            start_node["id_"]
        ]
        end_layout_element = node_id_to_layout_element_moved[end_node["id_"]]
        arc = map_builder.new_layout_element(
            relationship_type_to_cd_class[relationship.type]
        )
        start_point = start_layout_element.border(end_layout_element.center())
        end_point = end_layout_element.border(start_layout_element.center())
        arc.segments = momapy.core.TupleBuilder(
            [momapy.geometry.Segment(start_point, end_point)]
        )
        map_builder.layout.layout_elements.append(arc)
    for node_id in color_node_ids:
        layout_element_builder = node_id_to_layout_element_moved[node_id]
        layout_element_builder.line_width = 3.0
        layout_element_builder.stroke = momapy.coloring.yellow
        layout_element_builder.fill = momapy.coloring.red
    momapy.positioning.set_fit(layout_builder, layout_builder.layout_elements)
    momapy.rendering.core.render_map(
        map_builder, output_file_path, renderer="svg-native"
    )
