def find_key(data, key_to_find):
    """
    Find all values associated with a given key in a nested dictionary.

    Parameters
    ----------
    data : dict
        The nested dictionary to search.
    key_to_find : str
        The key to find in the dictionary.

    Returns
    -------
    list
        A list of values associated with the given key.

    Notes
    -----
    This function recursively searches a nested dictionary and returns all values associated with a given key.
    It supports dictionaries and lists as nested structures.

    Examples
    --------
    >>> data = {
    ...     "key1": "value1",
    ...     "key2": {
    ...         "key3": "value2",
    ...         "key4": "value3"
    ...     },
    ...     "key5": [
    ...         {"key6": "value4"},
    ...         {"key7": "value5"}
    ...     ]
    ... }
    >>> values = find_key(data, "key6")
    >>> values
    ['value4']
    """
    result = []

    def _find_key(data):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == key_to_find:
                    result.append(value)
                _find_key(value)
        elif isinstance(data, list):
            for item in data:
                _find_key(item)

    _find_key(data)
    return result


import ast


def parse_exception(error_string):
    """
    Parse an exception string and extract individual error messages.

    Parameters
    ----------
    error_string : str
        The exception string to parse.

    Returns
    -------
    list
        A list of parsed error messages.

    Notes
    -----
    This function parses an exception string and extracts individual error messages enclosed in curly braces.
    It can handle nested curly braces and skips any unparsable error messages.

    Examples
    --------
    >>> error_string = "{ 'error': 'Invalid input' } { 'error': 'Server error' }"
    >>> messages = parse_exception(error_string)
    >>> messages
    [{'error': 'Invalid input'}, {'error': 'Server error'}]
    """
    messages = []
    opening_brace_count = 0
    closing_brace_count = 0
    message_started = False
    message = ""

    for byte in error_string.encode():
        if byte == ord("{"):
            message += chr(byte)
            opening_brace_count += 1
            if opening_brace_count == closing_brace_count + 1:
                message_started = True
        elif byte == ord("}"):
            closing_brace_count += 1
            message += chr(byte)
            if closing_brace_count == opening_brace_count:
                message_started = False
                try:
                    messages.append(ast.literal_eval(message.strip()))
                except:
                    pass
                message = ""
        elif message_started:
            message += chr(byte)

    return messages
