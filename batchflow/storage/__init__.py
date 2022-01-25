_storage_obj = dict()

def get_storage(name, *args, **kwargs):
    global _storage_obj 
    if name == "backblaze":
        from .backblaze import BackBlazeStorage
        if _storage_obj.get("backblaze", None) is None:
            obj = BackBlazeStorage(*args, **kwargs)
            _storage_obj["backblaze"] = obj
            return obj
        else:
            return _storage_obj["backblaze"]
    elif name=="gdrive":
        from .gdrive import GDriveStorage
        if _storage_obj.get("gdrive", None) is None:
            obj = GDriveStorage(*args, **kwargs)
            _storage_obj["gdrive"] = obj
            return obj
        else:
            return _storage_obj["gdrive"]
    else:
        raise Exception(f"{name} storage not found")