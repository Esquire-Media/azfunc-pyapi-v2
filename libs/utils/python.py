# from dotenv import load_dotenv  # pip install as  `python-dotenv`
import ast, orjson as json, os, pandas as pd
from fuzzywuzzy import fuzz, process

# def load_env():
#     """
#     Load environment variables from a .env file.
#     """
#     env_path = Path('.') / '.env'
#     load_dotenv(dotenv_path=env_path)

def load_local_settings():
    """
    Load environment variables from a local.settings.json file.
    """
    with open('local.settings.json') as infile:
        env = json.load(infile)['Values']
        for key, value in env.items():
            os.environ[key] = value

def fuzzy_merge(left:pd.DataFrame, right:pd.DataFrame, left_on:str, right_on:str, threshold:int=90, limit:int=None, scorer=fuzz.ratio) -> pd.DataFrame:
    """
    Merge two dataframes on a string column using fuzzy matching with a given threshold.

    Params
    left : The left DataFrame to join
    right : The right DataFrame to join
    left_on : The key column for the left DataFrame
    right_on : The key column for the right DataFrame
    threshold: How close the threshold should be to return a match, based on fuzzywuzzy's Levenshtein distance
    limit: The maximum amount of matches that will get returned for a single record

    Returns
    m2 : A merged Pandas DataFrame indexed according to the left dataframe, with the right_index included as an integer column.

    """

    # get the top matches up to a certain limit per entry
    matches = left[left_on].apply(lambda x: process.extract(query=x, choices=right[right_on], limit=limit,  scorer=scorer))

    # explode each match tuple and filter by match strength
    m = pd.DataFrame(matches.explode())
    m['candidate'],m['ratio'],m['right_index'] = zip(*m[left_on])
    m = m.drop(columns=left_on)
    if threshold is not None:
        m = m[m['ratio']>=threshold]

    # merge back onto the left data
    m1 = pd.merge(
        left, 
        m,
        right_index=True,
        left_index=True
    )

    # merge back onto the right data
    m2 = pd.merge(
        m1,
        right,
        left_on='right_index',
        right_index=True
    )

    return m2.drop(columns=['candidate'])

def literal_eval_list(x:str):
    """
    Evaluate a string-formatted list back to a list, and if the value is None, set to an empty list.
    """
    if x == None:
        return []
    else:
        return ast.literal_eval(x)
    
def index_by_list(column, sort_list):
    """
    Utility function for indexing rows in a pandas dataframe using a list of sorted values.
    """
    correspondence = {item: idx for idx, item in enumerate(sort_list)}
    return column.map(correspondence)