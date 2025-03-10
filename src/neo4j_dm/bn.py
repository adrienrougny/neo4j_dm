import re


def make_bn_string_from_ig(ig):

    def make_variable_from_node(node):
        node_name = node["name"]
        normalized_node_name = re.sub("[^0-9a-zA-Z]", "_", node_name)
        return f"{node['id_']}_{normalized_node_name}"

    functions = ["targets, factors"]
    for node in ig.get_nodes():
        function_parts = []
        stimulators_function_part = " | ".join(
            [
                make_variable_from_node(stimulator)
                for stimulator in ig.get_stimulators(node)
            ]
        )
        if stimulators_function_part:
            stimulators_function_part = f"({stimulators_function_part})"
            function_parts.append(stimulators_function_part)
        necessary_stimulators_function_part = " & ".join(
            [
                make_variable_from_node(necessary_stimulator)
                for necessary_stimulator in ig.get_necessary_stimulators(node)
            ]
        )
        if necessary_stimulators_function_part:
            necessary_stimulators_function_part = (
                f"({necessary_stimulators_function_part})"
            )
            function_parts.append(necessary_stimulators_function_part)
        inhibitors_function_part = " | ".join(
            [
                make_variable_from_node(inhibitor)
                for inhibitor in ig.get_inhibitors(node)
            ]
        )
        if inhibitors_function_part:
            inhibitors_function_part = f"!({inhibitors_function_part})"
            function_parts.append(inhibitors_function_part)
        variable = make_variable_from_node(node)
        if function_parts:
            function = f"{variable}, {' & '.join(function_parts)}"
        else:
            function = f"{variable}, {variable}"
        functions.append(function)
    functions.append("")  # for bnet reading with biolqm
    return "\n".join(functions)


def make_bn_file_from_ig(ig, output_file_path):
    bn_string = make_bn_string_from_ig(ig)
    with open(output_file_path, "w") as f:
        f.write(bn_string)
