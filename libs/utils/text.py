import numpy as np
import re

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

def format_full_address(add1:str, add2:str):
    """
    Given an address 1 and address 2, return the formatted full address line.
    This method fixes some issues where the add2 line shows up as a string "NaN".
    """
    full_add = add1
    if type(add2)==str and len(add2) and add2.upper() not in ['NONE','NULL','NAN']:
        full_add += f' {add2}'
    return full_add

def format_zipcode(z):
    """
    Formats a zipcode by extracting the first 5-digit portion and adding leading zeroes if needed
    Returns null if no 5-digit portion can be extracted
    """
    try:
        z = str(z)
        z = re.findall('([0-9]{4,5})',str(z))[0]
        while len(z) < 5:
            z = '0' + z
        return z
    except IndexError:
        return None


def format_zip4(z):
    """
    Formats a 4-digit zip_plus_four_code
    """
    try:
        z = str(z)
        z = re.findall('([0-9]{3,4})',str(z))[0]
        while len(z) < 4:
            z = '0' + z
        return z
    except IndexError:
        return None