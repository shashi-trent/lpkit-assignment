from flask import current_app


class ConfigKeeper:
    def get(name:str):
        return current_app.config[name]
    
    def asInt(name:str):
        return int(ConfigKeeper.get(name))



