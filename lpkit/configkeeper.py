from os.path import join as osPathJoin
from flask import current_app


class ConfigKeeper:
    def get(name:str):
        return current_app.config[name]
    
    def asInt(name:str):
        return int(ConfigKeeper.get(name))
    
    def getReportFolderPath():
        return osPathJoin(current_app.root_path, ConfigKeeper.get('REPORT_CSVS_RELATIVE_PATH'))



