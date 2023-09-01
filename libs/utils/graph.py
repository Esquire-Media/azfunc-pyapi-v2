import json
import logging
import msal
import os
import requests
import jwt
from datetime import datetime, timezone, timedelta

from azure.identity import InteractiveBrowserCredential

class MicrosoftGraph:
    __cache = {}
    __token = None
    
    def __init__(
        self,
        scopes:list=['.default'],
        client_id:str=os.environ.get("graph_client_id"),
        client_credential:str=os.environ.get("graph_client_credential"),
        authority:str=os.environ.get("graph_authority")
    ):
        # Create an app instance which maintains a token cache.
        self.__scopes = scopes
        self.__client_id = client_id
        self.__client_credential = client_credential
        self.__authority = authority
        self.generate_token()
        
    
    def generate_token(self):
        # query the app
        if self.__client_credential:
            app = msal.ConfidentialClientApplication(
                client_id=self.__client_id, 
                authority=self.__authority,
                client_credential=self.__client_credential,
                token_cache=msal.TokenCache()
            )
            result = app.acquire_token_for_client(scopes=self.__scopes)
            if "access_token" in result:
                self.__token = result['access_token']
            else:
                logging.error([
                    "Graph Query Error:",
                    result.get("error"),
                    result.get("error_description"),
                    result.get("correlation_id")
                ])
                self.__token = None
        else:
            credential = InteractiveBrowserCredential()
            self.__token = credential.get_token(",".join(self.__scopes)).token
        self.token = self.__token
        logging.warning("Token Updated")
    
    def get_token(self):
        check = datetime.now(tz=timezone.utc)
        exp = datetime.fromtimestamp(jwt.decode(self.__token, options={"verify_signature": False})["exp"], tz=timezone.utc) - timedelta(minutes=5)
        if check.timestamp() > exp.timestamp():
            self.generate_token()
        return self.__token

            
    def clear_cache(self, path:str = None):
        if path:
            if path in self.__cache.keys():
                del self.__cache[path]
        else:
            self.__cache = {}
        
    def send(self, path:str, method:str="GET", payload:dict=None, version:str="v1.0", cache:bool=True, headers:dict={}):
        if not cache:
            self.clear_cache(f"{method} {path}")
            
        res = None
        if not f"{method} {path}" in self.__cache.keys():
            # logging.info(f"{method} {path}")
            self.__cache[f"{method} {path}"] = requests.request(
                method=method,
                url=f"https://graph.microsoft.com/{version}/{path}",
                headers={
                    'Authorization':f"Bearer {self.get_token()}",
                    **headers
                },
                json=payload
            )
            
        res = self.__cache[f"{method} {path}"]
            
        if not cache:
            del self.__cache[f"{method} {path}"]
        
        return res
    
    def send_bytes(self, path:str, method:str="GET", data:bytes=None, version:str="v1.0", cache:bool=True, headers:dict={}):
        if not cache:
            self.clear_cache(f"{method} {path}")
            
        res = None
        if not f"{method} {path}" in self.__cache.keys():
            # logging.info(f"{method} {path}")
            self.__cache[f"{method} {path}"] = requests.request(
                method=method,
                url=f"https://graph.microsoft.com/{version}/{path}",
                headers={
                    'Authorization':f"Bearer {self.get_token()}",
                    **headers
                },
                data=data
            )
            
        res = self.__cache[f"{method} {path}"]
            
        if not cache:
            del self.__cache[f"{method} {path}"]
        
        return res
    def query(self, path:str, method:str="GET", payload:dict=None, version:str="v1.0", cache:bool=True):
        res = self.send(path,method,payload,version,cache)
        try:
            value = json.loads(res.text)
            if "@odata.nextLink" in value.keys():
                next = self.query(path=value["@odata.nextLink"][29+len(version):],method=method, version=version, cache=cache)
                if "value" in next.keys():
                    value["value"].extend(next["value"])
            return value
        except:
            if "Content-Location" in res.headers.keys():
                return self.query(path=res.headers["Content-Location"][1:])
            else:
                logging.error(res.headers)
                logging.error(res.text)

def getFileList(graph, path):
    """
    given a Graph instance and a starting path, recursively lists all files under the starting path, along with their individual paths and download links.
    """
    # list will store returned file data
    file_list = []

    # get all children of the current path
    children = graph.query(f"{path}:/children")
    
    # check that children exist
    if 'value' in children.keys():
        
        for obj in children['value']:
            # if child is a file, add it to the list of files
            if 'file' in obj.keys():
                
                # file_list.append(obj)
                file_list.append({
                    'name':obj['name'],
                    'path':obj['parentReference']['path'],
                    'downloadUrl':obj['@microsoft.graph.downloadUrl']
                })
            # if child is a folder, recursively search it for more files
            elif 'folder' in obj.keys():
                [file_list.append(item) for item in getFileList(graph, f"{path}/{obj['name']}")]

    return file_list