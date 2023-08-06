"""
This program diffs delimtied files on a composite key
"""

import os
import sys
import csv
import io
import argparse
import json
from helpers import load_file_as_string
from helpers import infer_delimiter
from helpers import inject_composite_key
from comparison import _make_comparison



def delim_diff(file_a: str, file_b: str, delimiter: str = None, composite_key_fields: list = None, verbose: bool = False):
    """
    :param file_a: The first delimited file to compare
    :param file_b: The second delimited file to compare
    :param delimiter: The delimiter to use.  It nof passed, will be inferred
    :param composite_key_fields: A list of fields to use as the composite key.  If not passed, the first matched field
        This list of fields must be present in both files
    :param verbose: If True, will print verbose output
    :return:  #TODO:  Figure out what to return
    """

    """
    Validate and read files
    """
    files_list = [file_a, file_b]

    # Validate the files.  They should be real files
    for file in files_list:
        if not os.path.isfile(file):
            raise ValueError(f"{file} is not an actual file!")
        print(f"Validated that [{file}] is a real file.")

    # Load both files as strings
    file_a_str = load_file_as_string(file_a)
    file_b_str = load_file_as_string(file_b)

    """
    # Handle the delimiter
    """

    # If the delimiter is not specified, infer it
    if not delimiter:
        _inferred_delimiter_a = infer_delimiter(file_a_str)
        _inferred_delimiter_b = infer_delimiter(file_b_str)

        # Fail if the inferred delimiters from each file are different
        if _inferred_delimiter_a != _inferred_delimiter_b:
            raise ValueError(f"The inferred delimiters are different!  [{_inferred_delimiter_a}] and "
                             f"[{_inferred_delimiter_b}].  Please explicitly specify a delimiter then call the "
                             f"program again")
        else:
            delimiter = _inferred_delimiter_a
            print(f"Using inferred delimiter [{repr(delimiter)}]")
    else:
        if not type(delimiter) == str:
            print(f"Delimiter [{delimiter}] is not a string, but will be treated as one.", file=sys.stderr)
            delimiter = str(delimiter)
        print(f"Using specified delimiter [{repr(delimiter)}]")



    """
    Handle the header records / composite key fields
    """
    file_a_header = file_a_str.split('\n')[0]
    file_b_header = file_b_str.split('\n')[0]
    file_a_column_names = file_a_header.split(delimiter)
    file_b_column_names = file_b_header.split(delimiter)

    # Compare the column names between files
    matched_fields = []
    unmatched_fields = []
    for l in [file_a_column_names, file_b_column_names]:
        other_list = file_b_column_names if l is file_a_column_names else file_a_column_names
        for field in l:
            if field in other_list:
                if field not in matched_fields:
                    matched_fields.append(field)
            else:
                if field not in unmatched_fields:
                    unmatched_fields.append(field)

    if verbose is True:
        print(f"Matched fields: {matched_fields}")
        print(f"Unmatched fields: {unmatched_fields}")

    """
    Handle the the composite key 
    """

    # Ensure we're dealing with a list for composite_key_fields
    if composite_key_fields is None:
        composite_key_fields = []
    if type(composite_key_fields) is not list:
        composite_key_fields = [composite_key_fields]

    # Default to the first matched field if none are specified
    if not composite_key_fields:
        composite_key_fields = [matched_fields[0]]
        print(f"Will attempt to use [{composite_key_fields[0]}] as the composite key field since none were specified.  "
              f"This is the first matched (leftmost) field between both data sets.", file=sys.stderr)
    else:
        print(f"Using specified composite key fields {composite_key_fields}")

    # Ensure that all composite key fields are in the matched fields
    for field in composite_key_fields:
        if not field in matched_fields:
            raise ValueError(f"Composite key field [{field}] is not in the matched fields!")



    """
    Load the files as dictionaries
    """
    file_a_dict_reader = csv.DictReader(io.StringIO(file_a_str), delimiter=delimiter)
    file_b_dict_reader = csv.DictReader(io.StringIO(file_b_str), delimiter=delimiter)

    file_a_records = list(file_a_dict_reader)
    file_b_records = list(file_b_dict_reader)

    """
    Inject the composite key
    """
    inject_composite_key(file_a_records, composite_key_fields)
    inject_composite_key(file_b_records, composite_key_fields)

    """
    Do the comparison!!
    """
    #TODO:  Is there a way to use multiprocessing here?  https://docs.python.org/3/library/multiprocessing.html
    print("Starting comparison...")
    all_comparison_results = _make_comparison(list_of_dicts_a=file_a_records, list_of_dicts_b=file_b_records, verbose=verbose) # Compare A to B
    comparison_results = all_comparison_results['diffs']
    """
    Report statistics about the diffs
    """
    total_lines_with_diffs = len(comparison_results)
    field_level_diffs_running_total = 0
    present_in_a_not_in_b_running_total = 0
    present_in_b_not_in_a_running_total = 0

    for k in comparison_results.keys():

        result = comparison_results[k]

        # Count Field Level Diffs
        field_level_diffs = result.get('_field_differences_count')
        if field_level_diffs:
            field_level_diffs_running_total += field_level_diffs

        # Count Present in A not in B diffs
        present_in_a_not_in_b = result.get('_record_present_in_A_not_in_B')
        if present_in_a_not_in_b:
            present_in_a_not_in_b_running_total += 1

        # Count Present in B not in A diffs
        present_in_b_not_in_a = result.get('_record_present_in_B_not_in_A')
        if present_in_b_not_in_a:
            present_in_b_not_in_a_running_total += 1

    # print("\n[Details]:")
    # print(json.dumps(comparison_results, indent=4))

    print("\n\n[Summary]:")
    print(f"Lines in File A: {len(file_a_records)}")
    print(f"Lines in File B: {len(file_b_records)}")
    print(f"Unique composite keys across both files: {len(all_comparison_results['all_composite_keys'])}")
    print(f"Total lines with diffs: {total_lines_with_diffs}")
    print(f"Total field level diffs: {field_level_diffs_running_total}")
    print(f"Total rows present in A but not in B: {present_in_a_not_in_b_running_total}")
    print(f"Total rows present in B but not in A: {present_in_b_not_in_a_running_total}")


if __name__ == '__main__':
    # TODO:  Drop this testing kludge
    # TODO:  Parameterize for command line usage
    
    this_dir = os.path.dirname(os.path.realpath(__file__))
    testing_dir = os.path.join(this_dir, 'test_files')

    # # Lorem Ipsum unit tests
    file_1 = os.path.join(testing_dir,'test_file1.tsv')
    file_2 = os.path.join(testing_dir,'test_file2.tsv')
    delim_diff(file_a=file_1, file_b=file_2, verbose=True)

    # blink-182 vs Green Day unit tests
    # file_1 = os.path.join(testing_dir,'small_test_file1.tsv')
    # file_2 = os.path.join(testing_dir,'small_test_file2.tsv')
    # delim_diff(file_a=file_1, file_b=file_2, verbose=True, composite_key_fields=['Band_Name' , 'Instrument'])
    # delim_diff(file_a=file_1, file_b=file_2, verbose=True, composite_key_fields=['Instrument'])

