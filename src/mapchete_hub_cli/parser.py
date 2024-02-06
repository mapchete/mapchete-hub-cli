import datetime
import os
import py_compile
from collections import OrderedDict
from typing import Any, Optional, Union

import oyaml as yaml


def load_mapchete_config(
    mapchete_config: Union[dict, str], basedir: Optional[str] = None
) -> dict:
    """
    Return preprocessed mapchete config provided as dict or file.

    This function reads a mapchete config into an OrderedDict which keeps the item order
    statusd in the .mapchete file.
    If the configuration is passed on via a .mapchete file and if a process file path
    instead of a process module path was given, it will also check the syntax and replace
    the process item with the python code as string.

    Parameters
    ----------
    mapchete_config : str or dict
        A valid mapchete configuration either as path or dictionary.

    Returns
    -------
    OrderedDict
        Preprocessed mapchete configuration.
    """
    basedir = basedir or os.getcwd()
    if isinstance(mapchete_config, dict):
        conf = cleanup_dict(mapchete_config)
    elif isinstance(mapchete_config, str):
        basedir = os.path.dirname(mapchete_config)
        conf = cleanup_dict(yaml.safe_load(open(mapchete_config, "r").read()))
    else:  # pragma: no cover
        raise TypeError(
            "mapchete config must either be a path to an existing file or a dict"
        )

    process = conf.get("process")

    # handle process
    if not process:  # pragma: no cover
        raise KeyError("no or empty process in configuration")

    if isinstance(process, str):
        # local python file
        if process.endswith(".py"):
            custom_process_path = os.path.join(basedir, process)
            # check syntax
            py_compile.compile(custom_process_path, doraise=True)
            # assert file is not empty
            process_code = open(custom_process_path).read()
            if not process_code:  # pragma: no cover
                raise ValueError("process file is empty")
            conf.update(process=process_code.splitlines())

    return conf


def cleanup_dict(value: dict) -> OrderedDict:
    """Convert datetime objects in dictionary to strings."""
    return OrderedDict([(k, cleanup_datetime(v)) for k, v in value.items()])


def cleanup_datetime(value: Any) -> Any:
    """Convert datetime objects in dictionary to strings."""
    if isinstance(value, dict):
        return OrderedDict([(k, cleanup_datetime(v)) for k, v in value.items()])
    elif isinstance(value, (list, tuple)):
        return [cleanup_datetime(ii) for ii in value]
    elif isinstance(value, datetime.date):
        return str(value)
    else:
        return value
