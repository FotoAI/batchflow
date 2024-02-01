_storage_obj = dict()


def _init_storage(name, *args, **kwargs):
    if name == "backblaze":
        from .backblaze import BackBlazeStorage

        storage_obj = BackBlazeStorage(*args, **kwargs)

    elif name == "gdrive" or name == "google_drive":
        from .gdrive import GDriveStorage

        storage_obj = GDriveStorage(*args, **kwargs)

    elif name == "s3":
        from .s3 import S3

        storage_obj = S3(*args, **kwargs)

    else:
        raise Exception(f"{name} storage not found")
    return storage_obj


def get_storage(name, force=False, temp=False, *args, **kwargs):
    global _storage_obj

    if not force:
        storage_obj = _storage_obj.get(name, None)
        if storage_obj:
            return storage_obj
        else:
            storage_obj = _init_storage(name, *args, **kwargs)
            if not temp:
                _storage_obj[name] = storage_obj
    else:
        storage_obj = _init_storage(name, *args, **kwargs)
        if not temp:
            _storage_obj[name] = storage_obj

    return storage_obj
