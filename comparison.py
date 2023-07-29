"""
This module contains the primary comparison algorithm
"""

def do_comparison(some_list_of_dicts, some_other_list_of_dicts):
    # TODO:  Docstring

    #TODO:  Add handling for unimportant fields

    # raise NotImplementedError("This function is not yet implemented!") # TODO:  Drop this line

    """
    Validate inputs
    """

    # Validate inputs are lists
    for obj in [some_list_of_dicts, some_other_list_of_dicts]:
        if type(obj) is not list:
            raise TypeError(f"Object [{obj}] is not a list!.  Expected a list of dicts!")

    # Validate that the contents of the lists are dicts
    for l in [some_list_of_dicts, some_other_list_of_dicts]:
        for d in l:
            if type(d) is not dict:
                raise TypeError(f"Encountered object of type [{type(d)}] in list.  Expected a list of dicts!\n{d}")

    # Create a list of all the keys in the dicts
    all_keys = []
    for l in [some_list_of_dicts, some_other_list_of_dicts]:
        for d in l:
            composite_key = d['_composite_key_hash']
            if composite_key not in all_keys:
                all_keys.append(composite_key)

    print("!")  #TODO:  Drop this line