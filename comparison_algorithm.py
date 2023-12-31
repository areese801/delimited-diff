"""
This module contains the primary comparison algorithm
"""
import sys

from Levenshtein import distance as levenshtein_distance

def _find_record_by_composite_key(list_of_dicts:list, composite_key:str) -> dict:
    """
    Searches a list of dicts for a record with a specific composite key
    :param list_of_dicts: List of dictionaries to search
    :param composite_key: The sought composite key
    :return:
    """
    ret_val = None

    for _dict in list_of_dicts:
        if _dict['__composite_key_hash'] == composite_key:
            ret_val = _dict

    if ret_val is None:
        raise ValueError(f"Failed to locate record with composite key [{composite_key}]")

    return ret_val

def _make_comparison(list_of_dicts_a:list, list_of_dicts_b:list, unimportant_fields:list = None,
                     verbose: bool = False, _multiprocessing_bucket_id: str = None) -> dict:
    """
    The primary comparison algorithm
    :param list_of_dicts_a: The first delimited file, represented as a list of dicts
    :param list_of_dicts_b: The second delimited file, represented as a list of dicts
    :param verbose: Set to true to print more information
    :param unimportant_fields: A list of fields that should be ignored when comparing records
    :param _multiprocessing_bucket_id: Just a string to be passed into this function if it's invoked in multiprocessing mode
        it helps with print statements
    :return: dict
    """

    """
    Validate inputs
    """

    # Validate inputs are lists
    for list_of_dicts in [list_of_dicts_a, list_of_dicts_b]:
        if type(list_of_dicts) is not list:
            raise TypeError(f"Object [{list_of_dicts}] is not a list!.  Expected a list of dicts!")

    # Coerce unimportant_fields to a list if it is not already
    if unimportant_fields is None:
        unimportant_fields = []
    elif type(unimportant_fields) is not list:
        unimportant_fields = [unimportant_fields]


    # Validate that the contents of the lists are dicts
    # if verbose is True:
    #     print(f"Validating that the contents of the lists are dicts...")
    # for list_of_dicts in [list_of_dicts_a, list_of_dicts_b]:
    #     for _dict in list_of_dicts:
    #         if type(_dict) is not dict:
    #             raise TypeError(f"Encountered object of type [{type(_dict)}] in list.  Expected a list of dicts!\n{_dict}")

    """
    # Interrogate both lists of dicts for the composite keys
    """

    # Create a list of all the keys in the dicts
    all_composite_keys = []
    for list_of_dicts in [list_of_dicts_a, list_of_dicts_b]:
        for _dict in list_of_dicts:
            composite_key = _dict['__composite_key_hash']
            if composite_key not in all_composite_keys:
                all_composite_keys.append(composite_key)

    """
    Validate that any unimportant field specified is an actual field in the files.
    """

    # Create a unique list of all the keys in the dicts
    if unimportant_fields:
        unique_dict_keys = []
        for list_of_dicts in [list_of_dicts_a, list_of_dicts_b]:
            for _dict in list_of_dicts:
                for key in _dict.keys():
                    if key not in unique_dict_keys:
                        unique_dict_keys.append(key)

    # Bump the list of unimportant fields
        for unimportant_field in unimportant_fields:
            if not unimportant_field in unique_dict_keys:
                raise ValueError(f"Unimportant field [{unimportant_field}] was is not a field in either file.  "
                                 f"Please check spelling and try again!")

    # If the list of composite keys is shorter than both of the list of dicts, the composite key is not actually unique
    if len(all_composite_keys) < len(list_of_dicts_a) and len(all_composite_keys) < len(list_of_dicts_b):
        raise ValueError(f"There are {len(all_composite_keys)} unique composite keys between both files but there "
                         f"are {len(list_of_dicts_a)} records in File A and {len(list_of_dicts_b)} records in File B.  "
                         f"This means that composite key is not reliable to infer uniqueness.  "
                         f"Please specify more fields and invoke the program again")

    # Diff time!
    matched_composite_keys = []
    unmatched_composite_keys_from_list_a = []
    unmatched_composite_keys_from_list_b = []
    diffs = {}  # This is where we'll keep all of the diffs we encounter along the way
    counter = 0
    for _composite_key in all_composite_keys:
        counter += 1

        # Print progress
        if _multiprocessing_bucket_id:
            # Start with newline in case print statements step on eachother.  better to have whitespace than concatenated messages
            progress_message = f"\nBucket {_multiprocessing_bucket_id} --> Processing composite key {counter} of {len(all_composite_keys)} ({round(counter / len(all_composite_keys) * 100, 2)}%))"
        else:
            progress_message = f"Processing composite key {counter} of {len(all_composite_keys)} ({round(counter/len(all_composite_keys)*100, 2)}%))"
        if verbose is True:
            print(progress_message)
        elif counter % 500 == 0 or counter >= len(all_composite_keys):
            print(progress_message)

        composite_key_exists_in_a = False
        composite_key_exists_in_b = False

        # Check for the composite key in both lists of dicts
        for list_of_dicts in [list_of_dicts_a, list_of_dicts_b]:
            for _dict in list_of_dicts:
                if _dict['__composite_key_hash'] == _composite_key:
                    if list_of_dicts is list_of_dicts_a:
                        composite_key_exists_in_a = True
                    else:
                        composite_key_exists_in_b = True

        if composite_key_exists_in_a is False and composite_key_exists_in_b is False:
            # This should never occur.   If it does, it's a bug
            raise ValueError(f"Composite key [{_composite_key}] does not exist in either list!")

        # Track the matched and unmatched composite keys
        if composite_key_exists_in_a is True and composite_key_exists_in_b is True:
            # Both lists have the composite key

            matched_composite_keys.append(_composite_key)
        elif composite_key_exists_in_a is True and composite_key_exists_in_b is False:
            # Only list A has the composite key

            unmatched_composite_keys_from_list_a.append(_composite_key)
        elif composite_key_exists_in_a is False and composite_key_exists_in_b is True:
            # Only list B has the composite key

            unmatched_composite_keys_from_list_b.append(_composite_key)
        else:
            # We should never get here
            raise ValueError(f"Unexpected state!  composite_key_exists_in_a=[{composite_key_exists_in_a}] exists_in_b=[{composite_key_exists_in_b}]")

        # If the key exists in both of the lists, then we need to do field-level comparisons
        if composite_key_exists_in_a is True and composite_key_exists_in_b is True:

            # Locate record from data set A
            record_a = _find_record_by_composite_key(list_of_dicts=list_of_dicts_a, composite_key=_composite_key)
            record_a_composite_key = record_a['__composite_key_hash']
            record_a_composite_key_string = record_a['__composite_key_string']

            record_b = _find_record_by_composite_key(list_of_dicts=list_of_dicts_b, composite_key=_composite_key)
            record_b_composite_key = record_b['__composite_key_hash']
            record_b_composite_key_string = record_b['__composite_key_string']

            # Validate that we found the records
            if record_a is None or record_b is None:
                # This should never occur.  If it does, it's a bug
                raise ValueError(f"Failed to locate record for composite key [{_composite_key}]")

            # Create a list of keys that are in both records
            all_dict_keys = []
            for _key in list(record_a.keys()) + list(record_b.keys()):
                if _key not in all_dict_keys:
                    all_dict_keys.append(_key)

            # Create another dict that describes the diff
            row_key_exists_in_a = False
            row_key_exists_in_b = False
            for k in all_dict_keys:
                if k in unimportant_fields:
                    if verbose is True:
                        print(f"Skipping unimportant field [{k}] for row numbers: A) {record_a['__row_number']} and B) {record_b['__row_number']}")
                    continue

                # We don't need to handle the metadata keys inserted by this program
                if k in ['__composite_key_hash', '__composite_key_string', '__row_number']:
                    continue

                if k in record_a.keys():
                    row_key_exists_in_a = True
                if k in record_b.keys():
                    row_key_exists_in_b = True

                if row_key_exists_in_a is True and row_key_exists_in_b is True:
                    # Both records have the key.  Compare the values
                    record_a_value = record_a.get(k) # Row might be common, but not necessarily fields
                    record_b_value = record_b.get(k) # Row might be common, but not necessarily fields

                    if record_a_value != record_b_value:

                        # See that the composite key is in the larger diffs object (if this is the first time we've seen it)
                        if _composite_key not in diffs.keys():
                            diffs[_composite_key] = {}
                            diffs[_composite_key]['__field_differences_count'] = 0
                            diffs[_composite_key]['__composite_key_hash'] = record_a_composite_key # Will match b
                            diffs[_composite_key]['__composite_key_string'] = record_a_composite_key_string # Will match b


                        # diffs[_composite_key][f"{k}_Diff_Type"] = 'Field Difference' # DROP This in favor of the next line
                        diffs[_composite_key][k] = {}
                        diffs[_composite_key][k]['__diff_type'] = 'Field Difference'
                        diffs[_composite_key]['__field_differences_count'] += 1
                        # diffs[_composite_key][f"{k}_A"] = record_a_value #TODO  Drop this in favor of next lines
                        # diffs[_composite_key][f"{k}_B"] = record_b_value #TODO  Drop this in favor of next lines
                        diffs[_composite_key][k]['A'] = record_a_value
                        diffs[_composite_key][k]['B'] = record_b_value
                        _levenshtein_distance = levenshtein_distance(str(record_a_value), str(record_b_value))
                        # diffs[_composite_key][f"{k}_LEVENSHTEIN_DISTANCE"] = _levenshtein_distance #TODO:  Drop this in favor of the next line
                        diffs[_composite_key][k]['__levenshtein_distance'] = _levenshtein_distance

                        if verbose is True:
                            print(f"\nComposite key [{record_a['__composite_key_string']}: {_composite_key}] has a mismatched field [{k}]."
                                  f"\n\tValue in A = [{record_a_value}] (row number {record_a['__row_number']})"
                                  f"\n\tValue in B = [{record_b_value}] (row number {record_b['__row_number']})"
                                  f"\n\tLevenshtein Distance = [{_levenshtein_distance}]")
                    else:
                        if verbose is True:
                            print(f"\nComposite key [{record_a['__composite_key_string']}: {_composite_key}] has a matched field [{k}]."
                                  f"\n\tValue in A = [{record_a[k]}] (row number {record_a['__row_number']})"
                                  f"\n\tValue in B = [{record_b[k]}] (row number {record_b['__row_number']})")

                elif row_key_exists_in_a is True and row_key_exists_in_b is False:
                    # The key exists in record_a but not in record_b
                    if verbose is True:
                        print(f"Composite key [{record_a['__composite_key_string']}: {_composite_key}] has a field [{k}] that exists in A but not in B.  Value in A=[{record_a[k]}]")

                    if _composite_key not in diffs.keys():
                        diffs[_composite_key] = {}
                        diffs[_composite_key]['__composite_key_hash'] = record_a_composite_key
                        diffs[_composite_key]['__composite_key_string'] = record_a_composite_key_string

                    diffs[_composite_key][f"{k}_Diff_Type"] = 'Row In A but not in B'
                    diffs[_composite_key][f"{k}_A"] = record_a_value
                    diffs[_composite_key][f"{k}_B"] = None
                    diffs[_composite_key][f"{k}_LEVENSHTEIN_DISTANCE"] = len(record_a_value)
                elif row_key_exists_in_b is True and row_key_exists_in_a is False:
                    # The key exists in record_b but not in record_a
                    if verbose is True:
                        print(f"Composite key [{record_b['__composite_key_string']}: {_composite_key}] has a field [{k}] that exists in B but not in A.  Value in B=[{record_b[k]}]")

                    if _composite_key not in diffs.keys():
                        diffs[_composite_key] = {}
                        diffs[_composite_key]['__composite_key_hash'] = record_b_composite_key
                        diffs[_composite_key]['__composite_key_string'] = record_b_composite_key_string

                    diffs[_composite_key][f"{k}_Diff_Type"] = 'Row In B but not in A'
                    diffs[_composite_key][f"{k}_A"] = None
                    diffs[_composite_key][f"{k}_B"] = record_b_value
                    diffs[_composite_key][f"{k}_LEVENSHTEIN_DISTANCE"] = len(record_b_value)
                else:
                    # We should never get here
                    raise ValueError(f"Unexpected state!  row_key_exists_in_a=[{row_key_exists_in_a}] "
                                     f"row_key_exists_in_b=[{row_key_exists_in_b}]")
        elif composite_key_exists_in_a is True and composite_key_exists_in_b is False:
            # The key exists in record_a but not in record_b
            record_a = _find_record_by_composite_key(list_of_dicts=list_of_dicts_a, composite_key=_composite_key)

            if verbose is True:
                print(f"Composite key [{record_a['__composite_key_string']}: {_composite_key}] exists in A (row number {record_a['__row_number']}) but not in B.")

            if _composite_key not in diffs.keys():
                diffs[_composite_key] = {}
                diffs[_composite_key]['_record_present_in_A_not_in_B'] = True

            for k in record_a.keys():
                diffs[_composite_key][f"{k}_A"] = record_a[k]
                diffs[_composite_key][f"{k}_B"] = None
                diffs[_composite_key][f"{k}_LEVENSHTEIN_DISTANCE"] = len(str(record_a[k]))
        elif composite_key_exists_in_a is False and composite_key_exists_in_b is True:
            # The key exists in record_b but not in record_a
            record_b = _find_record_by_composite_key(list_of_dicts=list_of_dicts_b, composite_key=_composite_key)

            if verbose is True:
                print(f"Composite key [{record_b['__composite_key_string']}: {_composite_key}] exists in B (row number {record_b['__row_number']}) but not in A.")


            if _composite_key not in diffs.keys():
                diffs[_composite_key] = {}
                diffs[_composite_key]['_record_present_in_B_not_in_A'] = True

            for k in record_b.keys():
                diffs[_composite_key][f"{k}_A"] = None
                diffs[_composite_key][f"{k}_B"] = record_b[k]
                diffs[_composite_key][f"{k}_LEVENSHTEIN_DISTANCE"] = len(str(record_b[k]))
        else:
            # We should never get here
            raise ValueError(f"Unexpected state!  composite_key_exists_in_a=[{composite_key_exists_in_a}] "
                             f"composite_key_exists_in_b=[{composite_key_exists_in_b}]")

    ret_val = dict(diffs=diffs,
                   unmatched_composite_keys_from_list_a=unmatched_composite_keys_from_list_a,
                   unmatched_composite_keys_from_list_b=unmatched_composite_keys_from_list_b,
                   matched_composite_keys=matched_composite_keys,
                   all_composite_keys=all_composite_keys)

    return ret_val
