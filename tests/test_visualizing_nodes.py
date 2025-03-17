import os.path
import copy

import pygraphviz
import momapy.celldesigner.io.celldesigner
import momapy.io
import momapy.builder
import momapy_kb.neo4j.core
import momapy.rendering.svg_native
import momapy.rendering.core
import momapy.styling
import momapy.celldesigner.core
import momapy.core
import momapy.geometry
import momapy.drawing
import momapy.positioning

import neo4j_dm.queries
import neo4j_dm.ig
import neo4j_dm.core

import credentials


def translate_layout_element(layout_element, tx, ty):
    layout_element.position = layout_element.position + (tx, ty)
    for sub_layout_element in layout_element.children():
        translate_layout_element(sub_layout_element, tx, ty)


def get_layout_elements_from_node(node, map_, ids, model_id, collection_name):
    node_ids_and_context = neo4j_dm.queries.get_ids_and_context([node])
    node_id = None
    for node, ids_and_contexts in node_ids_and_context:
        for id_and_context in ids_and_contexts:
            candidate_node_id = id_and_context[0]
            entry_node = id_and_context[1]
            collection_node = id_and_context[2]
            if (
                collection_node["name"] == collection_name
                and entry_node["id_"] == model_id
            ):
                node_id = candidate_node_id
                break
    if node_id is None:
        return None
    model_element = None
    for candidate_model_element in ids:
        if node_id in ids[candidate_model_element]:
            model_element = candidate_model_element
            break
    if model_element is None:
        return None
    return map_.layout_model_mapping.get_mapping(model_element)


def get_layout_elements_from_id(id_, map_, ids):
    model_element = None
    for candidate_model_element in ids:
        if id_ in ids[candidate_model_element]:
            model_element = candidate_model_element
            break
    if model_element is None:
        return None
    return map_.layout_model_mapping.get_mapping(model_element)


relationship_type_to_cd_class = {
    "_POSITIVE_INFLUENCE": momapy.celldesigner.core.PositiveInfluenceLayout,
    "_NECESSARY_POSITIVE_INFLUENCE": momapy.celldesigner.core.TriggeringLayout,
    "_REACTANT_TO_PRODUCT": momapy.celldesigner.core.TriggeringLayout,
    "_NEGATIVE_INFLUENCE": momapy.celldesigner.core.InhibitionLayout,
}


if __name__ == "__main__":
    INPUT_FILE_PATH = "/home/rougny/research/commute/commute_dm_develop/build/data/pd_dm/celldesigner/Core_PD_map.xml"
    # INPUT_FILE_PATH = "/home/rougny/research/commute/commute_dm_develop/build/data/pd_dm/celldesigner/Inflammation_signaling.xml"
    COLLECTION_NAME = "PD_DM_CD"
    PHENOTYPE_NAME = "autophagy"
    # PHENOTYPE_NAME = "apoptosis"
    SAVE_MAP = False
    momapy_kb.neo4j.core.connect(
        credentials.URI, credentials.USERNAME, credentials.PASSWORD
    )
    if SAVE_MAP:
        neo4j_dm.core.save_collection_from_file_paths(
            COLLECTION_NAME, [INPUT_FILE_PATH], delete_all=True
        )
        neo4j_dm.ig.make_ig_in_db()
    model_id, _ = os.path.splitext(os.path.basename(INPUT_FILE_PATH))
    query = f"""
        MATCH (entry:CollectionEntry {{id_: "{model_id}"}})-[:HAS_MODEL]->(model:CellDesignerModel)-[:HAS_SPECIES]->(phenotype:Phenotype {{name: "{PHENOTYPE_NAME}"}}) RETURN phenotype
    """
    result, _ = momapy_kb.neo4j.core.run(query)

    phenotype_node = result[0][0]
    read_result = momapy.io.read(INPUT_FILE_PATH)
    map_ = read_result.obj
    ids = read_result.ids
    style_sheet_str = """
        .LayoutElement {
            stroke: unset;
            fill: none;
            path_stroke: unset;
            end_arrowhead_stroke: unset;
            start_arrowhead_stroke: unset;
            arrowhead_stroke: unset;
            reaction_node_stroke: unset;
            active_stroke: unset;
            group_stroke: lightgray;
        }

        ProductionLayout {
            arrowhead_stroke: lightgray;
            arrowhead_fill: lightgray;
        }

        StateTransitionLayout, HeterodimerAssociationLayout, KnownTransitionOmittedLayout, Transport, Translation, Transcription {
            end_arrowhead_stroke: lightgray;
            end_arrowhead_fill: lightgray;
            start_arrowhead_stroke: lightgray;
            start_arrowhead_fill: lightgray;
        }


    """
    style_sheet = momapy.styling.StyleSheet.from_string(style_sheet_str)
    nodes, relationships = neo4j_dm.queries.get_subgraph(
        node=phenotype_node,
        relationship_types=neo4j_dm.ig.INFLUENCES,
        # exclude_labels=["SimpleMolecule", "Ion"],
        mode="upstream",
        filter_output_relationships=True,
    )
    node_to_layout_elements = {}
    for node in nodes:
        layout_elements = get_layout_elements_from_node(
            node, map_, ids, model_id, COLLECTION_NAME
        )
        node_to_layout_elements[node] = layout_elements
        for layout_element in layout_elements:
            layout_element = next(iter(layout_element))
            style_sheet[momapy.styling.IdSelector(layout_element.id_)] = (
                momapy.styling.StyleCollection(
                    {"group_stroke": momapy.coloring.black}
                )
            )
            style_sheet[
                momapy.styling.ChildSelector(
                    momapy.styling.IdSelector(layout_element.id_),
                    momapy.styling.TypeSelector("TextLayout"),
                )
            ] = momapy.styling.StyleCollection(
                {
                    "stroke": momapy.drawing.NoneValue,
                    "fill": momapy.coloring.black,
                }
            )
    # map_builder_ig = momapy.builder.builder_from_object(map_)
    # style_sheet_ig = copy.deepcopy(style_sheet)
    # for relationship in relationships:
    #     start_node = relationship.start_node
    #     end_node = relationship.end_node
    #     start_layout_elements = get_layout_elements_from_node(
    #         start_node, map_, ids, model_id, COLLECTION_NAME
    #     )
    #     end_layout_elements = get_layout_elements_from_node(
    #         end_node, map_, ids, model_id, COLLECTION_NAME
    #     )
    #     for start_layout_element in start_layout_elements:
    #         start_layout_element = next(iter(start_layout_element))
    #         for end_layout_element in end_layout_elements:
    #             end_layout_element = next(iter(end_layout_element))
    #             arc = map_builder_ig.new_layout_element(
    #                 relationship_type_to_cd_class[relationship.type]
    #             )
    #             start_point = start_layout_element.border(
    #                 end_layout_element.center()
    #             )
    #             end_point = end_layout_element.border(
    #                 start_layout_element.center()
    #             )
    #             arc.segments = momapy.core.TupleBuilder(
    #                 [momapy.geometry.Segment(start_point, end_point)]
    #             )
    #             map_builder_ig.layout.layout_elements.append(arc)
    #             style_sheet_ig[momapy.styling.IdSelector(arc.id_)] = (
    #                 momapy.styling.StyleCollection(
    #                     {"group_stroke": momapy.coloring.black}
    #                 )
    #             )
    # map_ig = momapy.styling.apply_style_sheet(
    #     map_builder_ig, style_sheet_ig, strict=False
    # )
    # output_file_path = f"{model_id}_{PHENOTYPE_NAME}_ig.svg"
    # momapy.rendering.core.render_map(
    #     map_ig, output_file_path, renderer="svg-native"
    # )
    #
    # map_builder_reactions = momapy.builder.builder_from_object(map_)
    # style_sheet_reactions = copy.deepcopy(style_sheet)
    # for relationship in relationships:
    #     reaction_ids = relationship["reaction_ids"]
    #     for reaction_id in reaction_ids:
    #         layout_elements = get_layout_elements_from_id(
    #             reaction_id, map_, ids
    #         )
    #         for layout_element in layout_elements:
    #             layout_element = next(iter(layout_element))
    #             style_sheet_reactions[
    #                 momapy.styling.IdSelector(layout_element.id_)
    #             ] = momapy.styling.StyleCollection(
    #                 {"group_stroke": momapy.coloring.black}
    #             )
    #             style_sheet_reactions[
    #                 momapy.styling.ChildSelector(
    #                     momapy.styling.IdSelector(layout_element.id_),
    #                     momapy.styling.ClassSelector("LayoutElement"),
    #                 )
    #             ] = momapy.styling.StyleCollection(
    #                 {"group_stroke": momapy.coloring.black}
    #             )
    # map_reactions = momapy.styling.apply_style_sheet(
    #     map_builder_reactions, style_sheet_reactions, strict=False
    # )
    # output_file_path = f"{model_id}_{PHENOTYPE_NAME}_reactions.svg"
    # momapy.rendering.core.render_map(
    #     map_reactions, output_file_path, renderer="svg-native"
    # )
    node_to_layout_element = {}
    for node, layout_elements in node_to_layout_elements.items():
        layout_element = next(iter(next(iter(layout_elements))))
        node_to_layout_element[node] = layout_element
    map_builder = momapy.celldesigner.core.CellDesignerMapBuilder()
    layout_builder = map_builder.new_layout()
    map_builder.layout = layout_builder
    a_graph = pygraphviz.AGraph()
    node_id_to_coordinates = {}
    for relationship in relationships:
        a_graph.add_edge(
            relationship.start_node["id_"], relationship.end_node["id_"]
        )
    for node, layout_element in node_to_layout_element.items():
        a_node = a_graph.get_node(node["id_"])
        a_node.attr["width"] = layout_element.width / 96
        a_node.attr["height"] = layout_element.height / 96
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
    for relationship in relationships:
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
        # style_sheet_ig[momapy.styling.IdSelector(arc.id_)] = (
        #     momapy.styling.StyleCollection(
        #         {"group_stroke": momapy.coloring.black}
        #     )
        # )
    momapy.positioning.set_fit(layout_builder, layout_builder.layout_elements)
    output_file_path = f"{model_id}_{PHENOTYPE_NAME}_ig_full.svg"
    momapy.rendering.core.render_map(
        map_builder, output_file_path, renderer="svg-native"
    )
