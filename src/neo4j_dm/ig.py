import copy

import momapy_kb.neo4j.core

import neo4j_dm.utils

REACTANT_TO_PRODUCT = "_REACTANT_TO_PRODUCT"
POSITIVE_INFLUENCE = "_POSITIVE_INFLUENCE"
NEGATIVE_INFLUENCE = "_NEGATIVE_INFLUENCE"
NECESSARY_POSITIVE_INFLUENCE = "_NECESSARY_POSITIVE_INFLUENCE"
UNCERTAIN_POSITIVE_INFLUENCE = "_UNCERTAIN_POSITIVE_INFLUENCE"
UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE = (
    "_UNCERTAIN_NECESSARY_POSITIVE_INFLUENCE"
)
UNCERTAIN_NEGATIVE_INFLUENCE = "_UNCERTAIN_NEGATIVE_INFLUENCE"

RELATIONSHIPS = [
    REACTANT_TO_PRODUCT,
    POSITIVE_INFLUENCE,
    NEGATIVE_INFLUENCE,
    NECESSARY_POSITIVE_INFLUENCE,
]


class InfluenceGraph(dict):

    def get_nodes(self):
        return set(self.keys())

    def add_node(self, node):
        self[node] = set()

    def add_relationship(self, relationship):
        self[relationship.end_node].add(relationship)

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
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_MODULATIONN]->(modulation),
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
                    (entry:CollectionEntry)-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_MODULATIONN]->(modulation),
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
        ig.add_node(node)
    for relationship in relationships:
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
