def merge_dicts(d1, d2):
    for key, value in d2.items():
        if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
            merge_dicts(d1[key], value)
        else:
            d1[key] = value
    return d1

def nested_key_exists(d, keys):
    """
    Check if nested keys exist in a dictionary.
    
    :param d: The dictionary to check.
    :param keys: A list of keys, in the order of nesting.
    :return: True if all nested keys exist, False otherwise.
    """
    if not keys:
        return True

    key = keys[0]

    if key in d:
        return nested_key_exists(d[key], keys[1:])
    else:
        return False