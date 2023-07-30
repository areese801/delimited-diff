"""
Helper functions for the main program
"""

import os
import hashlib

def load_file_as_string(file_name:str) -> str:
    """
    Loads a file as a string and returns it.
    That's it.  That's all it does
    """

    # Validate the file
    if not os.path.isfile(file_name):
        raise ValueError(f"{file_name} is not an actual file!")

    # Load the file as a string
    with open(file_name, 'r') as file:
        file_str = file.read()

    # Return the value
    ret_val = file_str
    return ret_val

def infer_delimiter(input_string:str) -> str:
    """
    Infers the delimiter of a delimited file
    """

    # Split the string by newlines.  We only need the first row
    first_row = input_string.split('\n')[0]

    max_len = 0
    best_guess_delimiter = None

    for potential_delimiter in [",", "\t", "|"]:
        l = first_row.split(potential_delimiter)
        _len = len(l)
        if _len > max_len:
            max_len = _len
            best_guess_delimiter = potential_delimiter

    # Return the value
    if not best_guess_delimiter:
        raise ValueError("Failed to infer delimiter!")
    else:
        print(f"The inferred delimiter is [{best_guess_delimiter}].  It resulted in a split of {max_len} fields.")
        ret_val = best_guess_delimiter
        return ret_val

def inject_composite_key(data_object, composite_keys, verbose=False):
    """
    Concatenates and hashes the composite keys found in some dictionary to create a key
    This key is written into the dictionary
    :type data_object: A list of dicts or a dict
    """

    """
    Handle the data object
    """

    # Validate the data_object
    if not type(data_object) in [dict, list]:
        raise ValueError(f"Data object [{data_object}] is not a dictionary or a list of dictionaries!")

    # Ensure we're operating on a list of dictionaries even if it's just 1 entry
    if type(data_object) is dict:
        data_object = [data_object]

    """
    Inject the composite key into the data object
    """
    for _dict in data_object:

        # Validate that it's actually a dict
        if not type(_dict) is dict:
            raise ValueError(f"Data object [{_dict}] is not a dictionary!  It's a [{type(_dict)}]!")

        composite_key_string = ""
        for composite_key in composite_keys:
            if composite_key not in _dict.keys():
                raise ValueError(f"Composite key [{composite_key}] is not in the dictionary!  Keys found: [{_dict.keys()}]")
            composite_key_string += str(_dict[composite_key])

        if composite_key_string == "":
            raise ValueError(f"Failed to create a composite key string for [{_dict}]")

        composite_key_string = composite_key_string.lower().strip()

        # Hash the composite key string
        sha256_hash = hashlib.sha256()
        sha256_hash.update(composite_key_string.encode('utf-8'))
        composite_key_hash = sha256_hash.hexdigest()

        if verbose is True:
            print(f"Calculated composite key hash [{composite_key_hash}] for composite key string [{composite_key_string}]")

        # Inject the composite key hash into the dictionary
        for new_keys in ['_composite_key_hash', '_composite_key_string']:
            if new_keys in _dict.keys():
                raise ValueError(f"Key [{new_keys}] already exists in the dictionary!")
        _dict['_composite_key_hash'] = composite_key_hash
        _dict['_composite_key_string'] = composite_key_string








