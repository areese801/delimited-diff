"""
This program diffs delimtied files on a composite key
"""

import os
import sys
import csv
from helpers import load_file_as_string
from helpers import infer_delimiter
from helpers import inject_composite_key
from comparison import do_comparison

def main(file_a:str, file_b:str, delimiter=None, composite_key_fields=None):
    """
    This is the main program
    """

    """
    Validate read files
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
    if not delimiter:
        _inferred_delimiter_a = infer_delimiter(file_a_str)
        _inferred_delimiter_b = infer_delimiter(file_b_str)
        if _inferred_delimiter_a != _inferred_delimiter_b:
            raise ValueError(f"The inferred delimiters are different!  [{_inferred_delimiter_a}] and "
                             f"[{_inferred_delimiter_b}].  Please explicitly specify a delimiter then call the "
                             f"program again")
        else:
            delimiter = _inferred_delimiter_a
            print(f"Using inferred delimiter [{delimiter}]")
    else:
        if not type(delimiter) == str:
            print(f"Delimiter [{delimiter}] is not a string, but will be treated as one.", file=sys.stderr)
            delimiter = str(delimiter)
        print(f"Using specified delimiter [{delimiter}]")

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

    # Print the results
    print(f"Matched fields: {matched_fields}")
    print(f"Unmatched fields: {unmatched_fields}")


    """
    Handle the the composite key 
    """
    if composite_key_fields is None:
        composite_key_fields = []
    if type(composite_key_fields) is not list:
        composite_key_fields = list(composite_key_fields)


    # Default to the first matched field if none are specified
    if not composite_key_fields:
        composite_key_fields = [matched_fields[0]]
        print(f"Using [{composite_key_fields}] as the composite key field since none were specified.")

    # Ensure that all composite key fields are in the matched fields
    for field in composite_key_fields:
        if not field in matched_fields:
            raise ValueError(f"Composite key field [{field}] is not in the matched fields!")

    """
    Load the files as dictionaries
    """
    file_a_dict_reader = csv.DictReader(file_a_str.split('\n'), delimiter=delimiter)
    file_b_dict_reader = csv.DictReader(file_b_str.split('\n'), delimiter=delimiter)
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
    do_comparison(file_a_records, file_b_records) # Compare A to B

    print("!")








if __name__ == '__main__':
    # TODO:  Drop this testing kludge
    
    this_dir = os.path.dirname(os.path.realpath(__file__))
    testing_dir = os.path.join(this_dir, 'test_files')
    
    file_1 = os.path.join(testing_dir,'test_file1.tsv')
    file_2 = os.path.join(testing_dir,'test_file2.tsv')

    main(file_a=file_1, file_b=file_2)
