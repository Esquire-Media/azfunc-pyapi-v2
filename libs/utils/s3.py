import os
import re
from datetime import datetime as dt, timedelta

def s3_path_to_azure_path(key):
    """
    Convert the s3 filepath to a formatted Azure filepath.
    """
    # extract the MDY date string from the filename
    date_strings = re.search('[0-9]{8}', os.path.basename(key))
    # convert to YMD date string with underscores
    date = dt.strptime(date_strings[0],'%m%d%Y').date()

    if "CNM" in os.path.basename(key):
        return f"movers/esquire-movers_{date.strftime('%Y_%m_%d')}"
    elif "PREMOVER" in os.path.basename(key):
        return f"premovers/esquire-premovers_{date.strftime('%Y_%m_%d')}"
    else:
        return ''
    
def is_fresher_than_six_months(file:str):
    """
    Check if an s3 filepath has a datestring of format DDMMYYYY that is fresher than six months.
    """
    if re.search("[0-9]{8}",file):
        if dt.strptime(re.search("[0-9]{8}",file)[0], '%m%d%Y') >= (dt.today()-timedelta(weeks=24)):
            return True
        else:
            return False
    else:
        return False