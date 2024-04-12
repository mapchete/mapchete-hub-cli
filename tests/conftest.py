import os
from collections import namedtuple

import pytest
import yaml
from click.testing import CliRunner
from fastapi.testclient import TestClient
from mapchete_hub.app import app

from mapchete_hub_cli import Client
from mapchete_hub_cli.cli import mhub

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="session")
def mhub_client(client):
    return Client(_test_client=client)


@pytest.fixture
def mhub_integration_client():
    return Client(os.environ.get("MHUB_HOST", "http://0.0.0.0:5000"))


@pytest.fixture
def cli():
    class CLI:
        def __init__(self, cli_func):
            self.cli_func = cli_func

        def run(self, command):
            result = CliRunner().invoke(
                self.cli_func,
                [
                    "--host",
                    os.environ.get("MHUB_HOST", "http://0.0.0.0:5000"),
                    *command.split(" "),
                ],
            )
            if result.exit_code != 0:
                print(result.output)
            return result

    return CLI(mhub)


@pytest.fixture
def test_process_id():
    return "mapchete.processes.convert"


@pytest.fixture
def example_config_json(tmpdir):
    return {
        "command": "execute",
        "params": {"zoom": 5, "bounds": [0, 1, 2, 3]},
        "config": {
            "process": "mapchete.processes.convert",
            "input": {
                "inp": "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"
            },
            "output": {
                "format": "GTiff",
                "bands": 1,
                "dtype": "uint16",
                "path": str(tmpdir),
            },
            "pyramid": {"grid": "geodetic", "metatiling": 2},
            "zoom_levels": {"min": 0, "max": 13},
        },
    }


@pytest.fixture
def example_config_custom_process_json(tmpdir):
    return {
        "command": "execute",
        "params": {"zoom": 8, "bounds": [0, 1, 2, 3]},
        "config": {
            "process": [
                "def execute(mp):",
                "    with mp.open('inp') as inp:",
                "        return inp.read()",
            ],
            "input": {
                "inp": "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"
            },
            "output": {
                "format": "GTiff",
                "bands": 1,
                "dtype": "uint16",
                "path": str(tmpdir),
            },
            "pyramid": {"grid": "geodetic", "metatiling": 2},
            "zoom_levels": {"min": 0, "max": 13},
        },
    }


@pytest.fixture
def example_config_python_process_json(tmpdir):
    return {
        "command": "execute",
        "params": {"zoom": 8, "bounds": [0, 1, 2, 3]},
        "config": {
            "process": "example.py",
            "input": {
                "inp": "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"
            },
            "output": {
                "format": "GTiff",
                "bands": 1,
                "dtype": "uint16",
                "path": str(tmpdir),
            },
            "pyramid": {"grid": "geodetic", "metatiling": 2},
            "zoom_levels": {"min": 0, "max": 13},
        },
    }


@pytest.fixture
def example_config_process_exception_json(tmpdir):
    return {
        "command": "execute",
        "params": {"zoom": 8, "bounds": [0, 1, 2, 3]},
        "config": {
            "process": [
                "def execute(mp):",
                "    1/0",
            ],
            "input": {
                "inp": "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"
            },
            "output": {
                "format": "GTiff",
                "bands": 1,
                "dtype": "uint16",
                "path": str(tmpdir),
            },
            "pyramid": {"grid": "geodetic", "metatiling": 2},
            "zoom_levels": {"min": 0, "max": 13},
        },
    }


@pytest.fixture
def example_config_mapchete(tmpdir):
    ExampleConfig = namedtuple("ExampleConfig", ("path", "dict"))
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "example.mapchete")
    temp_config_path = os.path.join(tmpdir, "example.mapchete")
    temp_output_path = os.path.join(tmpdir, "output")
    with open(path) as src:
        config = yaml.safe_load(src.read())
    config["output"].update(path=temp_output_path)
    with open(temp_config_path, "w") as dst:
        dst.write(yaml.dump(config))
    return ExampleConfig(temp_config_path, config)


@pytest.fixture
def custom_dask_specs_json():
    return os.path.join(SCRIPT_DIR, "custom_dask_specs.json")
