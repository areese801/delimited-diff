"""
This program diffs delimited files on a composite key
"""

import os
import sys
import csv
import io
import argparse
from itertools import product
from helpers import load_file_as_string
from helpers import infer_delimiter
from helpers import inject_composite_key
from comparison_algorithm import _make_comparison
from multiprocessing import Manager, Pool



def process_bucket(bucket: dict, shared_results_dict: dict):
    """
    This function is called by the multiprocessing pool.  It is the function that is called in parallel
    Args:
        bucket: The bucket to process
        shared_results_dict: The shared results dict
    Returns:
    """

    bucket_id = bucket['bucket_id']
    list_a = bucket['A']
    list_b = bucket['B']
    unimportant_fields = bucket['unimportant_fields']
    verbose = bucket['verbose']

    comparison_result = _make_comparison(list_of_dicts_a=list_a, list_of_dicts_b=list_b,
                                         unimportant_fields=unimportant_fields, verbose=verbose,
                                         _multiprocessing_bucket_id=bucket_id)
    shared_results_dict[bucket_id] = comparison_result

    return comparison_result  #TODO:  Is this line needed?  We're just relying on the shared obj here

def delim_diff(file_a: str, file_b: str, delimiter: str = None, composite_key_fields: list = None, unimportant_fields:list = None ,
               verbose: bool = False, use_multiprocessing: bool = True):
    """
    :param file_a: The first delimited file to compare
    :param file_b: The second delimited file to compare
    :param delimiter: The delimiter to use.  It nof passed, will be inferred
    :param composite_key_fields: A list of fields to use as the composite key.  If not passed, the first matched field
        This list of fields must be present in both files
    :param verbose: If True, will print verbose output
    :param use_multiprocessing: If True, multiprocessing will be used.  Note that multiprocessing is tremendously faster
        and should generally always be used unless this program is being debugged.
    :return:  dict of comparison results
    unimportant_fields : list = A list of fields to ignore when comparing rows
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
    Handle unimportant fields
    """
    if unimportant_fields is None:
        unimportant_fields = []
    elif type(unimportant_fields) is not list:
        unimportant_fields = [unimportant_fields]

    # Unimportant fields mustn't be in the composite key fields
    for field in unimportant_fields:
        if field in composite_key_fields:
            raise ValueError(f"Unimportant field [{field}] is in the composite key fields!  "
                             f"If it is part of the key, it cannot be specified as unimportant, which causes it to be ignored.")

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
    Bucketize the records for multiprocessing
    """

    if use_multiprocessing is True:
        print("Generate empty buckets for batching...")
        hex_chars = '0123456789abcdef'

        # TODO:  Drop this block, which is replaced by fewer lines below
        # buckets = {}
        # for h1 in hex_chars:
        #     for h2 in hex_chars:
        #         for h3 in hex_chars:
        #             bucket_id = f"{h1}{h2}{h3}"
        #             buckets[bucket_id] = dict(bucket_id=bucket_id, A=[], B=[])

        buckets = {f"{h1}{h2}{h3}": {'bucket_id': f"{h1}{h2}{h3}", 'A': [], 'B': []}
               for h1, h2, h3 in product(hex_chars, repeat=3)}

        print("Assigning records to buckets...")
        bucket_char_width = len(list(buckets.keys())[0])
        for rec_list in [file_a_records, file_b_records]:
            for rec in rec_list:
                bucket = rec['__composite_key_hash'][:bucket_char_width]
                if rec_list is file_a_records:
                    buckets[bucket]['A'].append(rec)
                else:
                    buckets[bucket]['B'].append(rec)
                buckets[bucket]['unimportant_fields'] = unimportant_fields
                buckets[bucket]['verbose'] = verbose

        """
        Do the comparison using multiprocessing
        """
        print("Starting comparison...")

        # Use Manager to create a shared object that the processes can all write to
        manager = Manager()  # This allows us to share a results object between processes
        _shared_results = manager.dict()
        for bk in buckets.keys():
            _shared_results[bk] = {}

        # A pool for keeping track of worker processes
        pool = Pool()
        for bk in buckets.keys():
            args = (buckets[bk], _shared_results)  # This must be a tuple, with positional arguments.  Chat GPT says so!
            pool.apply_async(process_bucket, args=args)

        pool.close()
        pool.join()
        shared_results = dict(_shared_results)  # Convert the shared results to a regular dict

        # Assemble shared_results to match all_comparison_results
        print("All processes have completed.  Collecting results...")

        # Re-construct a single results object from the shared results
        mp_all_comparison_results = {}
        mp_all_comparison_results['diffs'] = {}
        mp_all_comparison_results['unmatched_composite_keys_from_list_a'] = []
        mp_all_comparison_results['unmatched_composite_keys_from_list_b'] = []
        mp_all_comparison_results['matched_composite_keys'] = []
        mp_all_comparison_results['all_composite_keys'] = []

        for sr in shared_results.keys():
            # if verbose is True:
            #     print(f"Processing results for bucket {sr}...")  #TODO.  Drop this.  Who cares
            rec = shared_results[sr]
            _diffs = rec.get('diffs') or {}
            _unmatched_composite_keys_from_list_a = rec.get('unmatched_composite_keys_from_list_a') or []
            _unmatched_composite_keys_from_list_b = rec.get('unmatched_composite_keys_from_list_b') or []
            _matched_composite_keys = rec.get('matched_composite_keys') or []
            _all_composite_keys = rec.get('all_composite_keys') or []

            mp_all_comparison_results['diffs'].update(_diffs)
            mp_all_comparison_results['unmatched_composite_keys_from_list_a'].extend(_unmatched_composite_keys_from_list_a)
            mp_all_comparison_results['unmatched_composite_keys_from_list_b'].extend(_unmatched_composite_keys_from_list_b)
            mp_all_comparison_results['matched_composite_keys'].extend(_matched_composite_keys)
            mp_all_comparison_results['all_composite_keys'].extend(_all_composite_keys)

        # Report the results from the multiprocessing variant
        print("\n\n[Summary from Multiprocessing]:")
        total_lines_with_diffs = len(mp_all_comparison_results['diffs'])
        field_level_diffs_running_total = 0
        present_in_a_not_in_b_running_total = 0
        present_in_b_not_in_a_running_total = 0

        for k in mp_all_comparison_results['diffs'].keys():
            result = mp_all_comparison_results['diffs'][k]

            # Count Field Level Diffs
            field_level_diffs = result.get('__field_differences_count')
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

        print("\n\n[Summary]:")
        if len(unimportant_fields) > 0:
            print(f"--> SKIPPED over these unimportant fields: {unimportant_fields}")
        print(f"Lines in File A: {len(file_a_records)}")
        print(f"Lines in File B: {len(file_b_records)}")
        print(f"Unique composite keys across both files: {len(mp_all_comparison_results['all_composite_keys'])}")
        print(f"Total lines with diffs (Excluding Unimportant Fields): {total_lines_with_diffs}")
        print(f"Total field level diffs (Excluding Unimportant Fields): {field_level_diffs_running_total}")
        print(f"Total rows present in A but not in B: {present_in_a_not_in_b_running_total}")
        print(f"Total rows present in B but not in A: {present_in_b_not_in_a_running_total}")

        ret_val = mp_all_comparison_results['diffs']

    else:
        # Single Process Comparison.  Normally, we'll want to avoid this except for debugging, because it's slow.
        all_comparison_results = _make_comparison(list_of_dicts_a=file_a_records, list_of_dicts_b=file_b_records
                                                  , unimportant_fields=unimportant_fields, verbose=verbose) # Compare A to B
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
            field_level_diffs = result.get('__field_differences_count')
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

        print("\n\n[Summary]:")
        if len(unimportant_fields) > 0:
            print(f"--> SKIPPED over these unimportant fields: {unimportant_fields}")
        print(f"Lines in File A: {len(file_a_records)}")
        print(f"Lines in File B: {len(file_b_records)}")
        print(f"Unique composite keys across both files: {len(all_comparison_results['all_composite_keys'])}")
        print(f"Total lines with diffs (Excluding Unimportant Fields): {total_lines_with_diffs}")
        print(f"Total field level diffs (Excluding Unimportant Fields): {field_level_diffs_running_total}")
        print(f"Total rows present in A but not in B: {present_in_a_not_in_b_running_total}")
        print(f"Total rows present in B but not in A: {present_in_b_not_in_a_running_total}")

        ret_val = comparison_results

    return ret_val




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Compare two delimited files.')

    parser.add_argument('--file-a', '-a',
                        type=str,
                        required=True,
                        help='The first file (File A) to be used in the comparison.')
    parser.add_argument('--file-b', '-b',
                        type=str,
                        required=True,
                        help='The second file (File B) to be used in the comparison.')
    parser.add_argument('--delimiter', '-d',
                        type=str,
                        required=False,
                        default='\t',
                        help='The delimiter to use when parsing the files.  Default is tab.')
    parser.add_argument('--composite-key-fields', '-k',
                        type=str,
                        required=False,
                        nargs='+',
                        help='The field(s) to use as the composite key.  '
                             'If not specified, the first matched field will be used.')
    parser.add_argument('--unimportant-fields', '-u',
                        type=str,
                        required=False,
                        nargs='+',
                        help='The field(s) to ignore when comparing the files.')
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        required=False,
                        help='Prints out the details of the comparison.')
    parser.add_argument('--single-process', '-s',
                        action='store_true',
                        required=False,
                        help='Forces the comparison to run in a single process.  '
                             '(Not recommended except for debugging.)')

    args = parser.parse_args()
    use_multiprocessing = not args.single_process

    delim_diff(file_a=args.file_a,
               file_b=args.file_b,
               delimiter=args.delimiter,
               composite_key_fields=args.composite_key_fields,
               unimportant_fields=args.unimportant_fields,
               verbose=args.verbose,
               use_multiprocessing=use_multiprocessing)





