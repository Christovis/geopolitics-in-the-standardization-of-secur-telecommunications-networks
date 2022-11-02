import os
import yaml
from pathlib import Path

folder_project = str(Path(os.path.abspath(__file__)).parent.parent.parent.parent)
file_config = os.path.join(folder_project, "src/tgpp/config", "config.yml")
stream = open(file_config, "r")
dictionary = yaml.safe_load(stream)

class Config(object):
    def __init__(self, conf):
        #self.folder_project = folder_project
        self.CONFIG = conf

    def __getattr__(self, query):
        if query in self.CONFIG:
            ans = self.CONFIG[query]
            if ("folder" in query) or ("file" in query):
                if isinstance(ans, str):
                    ans = os.path.join(folder_project, ans)
                elif isinstance(ans, list):
                    ans = [os.path.join(folder_project, s) for s in ans]
            return ans
        else:
            return None

CONFIG = Config(dictionary)
