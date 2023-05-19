def find_key(data, key_to_find):
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