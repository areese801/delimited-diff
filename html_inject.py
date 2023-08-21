"""
This module accepts an output object as would be returned by comparison_algorithm and decorates the diff
records found therein with HTML. This HTML can later be rendered as a visualization of the diffs
"""

import os
def inject_diff_html(comparison_results:dict, verbose=False) -> dict:
    """
    Injects HTML into the diff records found in the comparison results
    :param comparison_results: The output of comparison_algorithm
    :param verbose: Whether to print verbose output
    :return: The comparison results with HTML injected
    """

    # Validate the input
    if not isinstance(comparison_results, dict):
        raise ValueError("comparison_results must be a dict")


    # Traverse the dictionary
    diffs = comparison_results['diffs']
    for composite_key in diffs.keys():
        for k in diffs[composite_key].keys():
            v = diffs[composite_key][k]
            if type(v) is not dict:
                # We expect the diffs to be wrapped up in dicts where each dict's name corresponds with a field from the input file
                continue

            # Quick check to ensure we've countered an object that adheres to our scheme
            if '__diff_type' not in v.keys():
                # This is unexpected
                raise ValueError(f"Expected the key '__diff_type' to be in the diff record, but it was not.  This is unexpected and is probably a bug.  The record is {v}")

            _diff_type = v['__diff_type']

            # Get the HTML corresponding with the difference type
            if _diff_type == 'Field Difference':
                _inject_html_for_field_difference(v)
            else:
                # We shouldn't get here.
                raise ValueError(f"Unexpected diff type [{_diff_type}]")

def _inject_html_for_field_difference(field_diff_dict:dict) -> str:
    """
    Returns an HTML string geneates based on the input field-level difference
    Args:
        field_diff_dict:
    Returns:
    """

    string_a = field_diff_dict['A']
    string_b = field_diff_dict['B']

    string_a_list = list(string_a)
    string_b_list = list(string_b)

    # A bit of flim flam to ensure that we're comparing the longer string to the shorter string
    string_x_list = string_a_list if len(string_a_list) >= len(string_b_list) else string_b_list
    string_y_list = string_b_list if string_x_list is string_a_list else string_a_list

    # compare string A to string B character by character
    colored_string_list_x = []
    colored_string_list_y = []
    idx = 0
    # min_len = min(len(string_x_list), len(string_y_list)) #TODO:  Drop this line
    # max_len = max(len(string_x_list), len(string_y_list)) #TODO:  Drop this line
    while idx < len(string_y_list):
        char_x = string_a_list[idx]
        char_y = string_b_list[idx]

        if char_x == char_y:
            color = 'black'
        elif char_x.lower() == char_y.lower():
            color = 'blue'
        elif char_x != char_y:
            color = 'red'
        else:
            raise ValueError(f"Unexpected condition.  char_x is [{char_x}] and char_y is [{char_y}]")

        html_string_x = f'<span style="color: {color};">{string_x_list[idx]}</span>'
        html_string_y = f'<span style="color: {color};">{string_y_list[idx]}</span>'
        colored_string_list_x.append(html_string_x)
        colored_string_list_y.append(html_string_y)

        idx += 1

    # At this point we've checked the specific characters up to the length of the shorter string
    # Fill in the rest of the characters for the longer string, x
    while idx < len(string_x_list):
        html_string_x = f'<span style="color: red;">{string_x_list[idx]}</span>'
        html_string_y = f'<span style="background-color: #FFCCCC;">&nbsp;</span>'

        colored_string_list_x.append(html_string_x)
        colored_string_list_y.append(html_string_y)

        idx += 1

    # Join the strings back together
    colored_string_x = ''.join(colored_string_list_x)
    colored_string_y = ''.join(colored_string_list_y)

    # Assign x, y back to a,b
    colored_string_a = colored_string_x if string_x_list is string_a_list else colored_string_y
    colored_string_b = colored_string_y if string_x_list is string_a_list else colored_string_x

    # Inject the colored strings into the diff dict
    field_diff_dict['__html_colored_string_a'] = colored_string_a
    field_diff_dict['__html_colored_string_b'] = colored_string_b

    # TODO:  Next block is testing kludge.  drop it
    this_dir = os.path.dirname(os.path.realpath(__file__))
    output_html_file = os.path.join(this_dir, 'test_files', 'test.html')
    with open(output_html_file, 'w') as f:
        f.write(f"<html><body><p>{colored_string_a}</p><p>{colored_string_b}</p></body></html>")
    print(f"Wrote test HTML into [{output_html_file}]")

    # TODO:  Return statement probably not needed here.  Mutability
    return dict(colored_string_a=colored_string_a, colored_string_b=colored_string_b)
