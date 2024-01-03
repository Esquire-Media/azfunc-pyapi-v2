from collections.abc import MutableMapping

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
    
    

def flatten(dictionary, parent_key='', separator='.'):
    """
    Flatten a dictionary by unpacking any nested objects into the parent object, with separators to indicate their original path.

    Example input:
    {
        'a': 1,
        'c': {
            'a': 2,
            'b': {
                'x': 5,
                'y' : 10
                }
            },
        'd': [1, 2, 3]
    }

    Example output:
    {
        'a': 1,
        'c.a': 2,
        'c.b.x': 5,
        'c.b.y': 10,
        'd': [1, 2, 3]
    }
    """
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)
