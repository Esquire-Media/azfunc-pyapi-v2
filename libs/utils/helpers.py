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


import ast


def parse_exception(error_string):
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
