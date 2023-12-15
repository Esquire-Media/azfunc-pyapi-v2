from typing import Dict, Union
import numpy as np, re


# Define a function to convert a string to camel case
def camel_case(s):
    # Use regular expression substitution to replace underscores and hyphens with spaces,
    # then title case the string (capitalize the first letter of each word), and remove spaces
    s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")

    # Join the string, ensuring the first letter is lowercase
    return "".join([s[0].lower(), s[1:]])


def pad_text(s: str, l: int):
    """
    Pad or truncate a string to an exact length.

    s: The string to adjust. Can be string or int.
    l: The desired length.
    """
    if type(s) == int or type(s) == np.int64:
        s = "{:,}".format(s)

    if len(s) == l:
        return s
    elif len(s) < l:
        return s + " " * (l - len(s))
    else:
        return s[: l - 3] + "..."


def format_num(n):
    return "{:,}".format(n)


def format_full_address(add1: str, add2: str):
    """
    Given an address 1 and address 2, return the formatted full address line.
    This method fixes some issues where the add2 line shows up as a string "NaN".
    """
    full_add = add1
    if type(add2) == str and len(add2) and add2.upper() not in ["NONE", "NULL", "NAN"]:
        full_add += f" {add2}"
    return full_add


def format_zipcode(z):
    """
    Formats a zipcode by extracting the first 5-digit portion and adding leading zeroes if needed
    Returns null if no 5-digit portion can be extracted
    """
    try:
        z = str(z)
        z = re.findall("([0-9]{4,5})", str(z))[0]
        while len(z) < 5:
            z = "0" + z
        return z
    except IndexError:
        return None


def format_zip4(z):
    """
    Formats a 4-digit zip_plus_four_code
    """
    try:
        z = str(z)
        z = re.findall("([0-9]{3,4})", str(z))[0]
        while len(z) < 4:
            z = "0" + z
        return z
    except IndexError:
        return None


def to_number(
    s: str, suffix_map: Dict[str, Union[int, float]] = None
) -> Union[int, float]:
    """
    Convert a string with a numerical value and a suffix to an integer or a float.

    This function handles strings representing numbers with various suffixes indicating
    large numbers or units, converting these strings into their numerical equivalents.
    If no suffix map is provided, a default map with various common suffixes is used.

    Parameters
    ----------
    s : str
        The string representing the numerical value with a suffix.
        For example, '50 K', '1.5 M', or '2 B'.
    suffix_map : dict, optional
        A dictionary mapping suffixes (str) to their corresponding multipliers (int or float).
        Default includes a wide range of suffixes from different contexts.

    Returns
    -------
    int or float
        The numerical representation of the input string as an integer or a float,
        depending on the suffix and the value. Suffixes that imply a fractional
        value result in a float.

    Examples
    --------
    >>> to_number("50 K")
    50000
    >>> to_number("1.5 M")
    1500000
    >>> to_number("2 B")
    2000000000
    >>> to_number("5 GB")
    5368709120
    >>> to_number("300")
    300
    >>> to_number("500m")
    0.5
    """
    # Default suffix map
    if suffix_map is None:
        suffix_map = {
            "k": 10**3,
            "K": 10**3,
            "M": 10**6,
            "MM": 10**6,
            "B": 10**9,
            "G": 10**9,
            "T": 10**12,
            "P": 10**15,
            "E": 10**18,
            "Z": 10**21,
            "Y": 10**24,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
            "PB": 1024**5,
            "EB": 1024**6,
            "c": 1 / 10**2,
            "%": 1 / 10**2,
            "m": 1 / 10**3,
            "‰": 1 / 10**3,
            "μ": 1 / 10**6,
            "u": 1 / 10**6,
            "n": 1 / 10**9,
            "p": 1 / 10**12,
        }

    for suffix, multiplier in suffix_map.items():
        if suffix in s:
            value = float(s.replace(" ", "").replace(suffix, "")) * multiplier
            return value if multiplier < 1 else int(value)
    return int(s)
