import numpy as np

def pad_text(s:str, l:int):
    """
    Pad or truncate a string to an exact length.

    s: The string to adjust. Can be string or int.
    l: The desired length.
    """
    if type(s) == int or type(s)==np.int64:
        s = "{:,}".format(s)

    if len(s) == l:
        return s
    elif len(s) < l:
        return s + ' '*(l - len(s))
    else:
        return s[:l-3] + '...'
    
def format_num(n):
    return "{:,}".format(n)