from mapchete import MPath

MHUB_TEST_BUCKET = MPath("s3://eox-mhub-cache")

MAPCHETE_TEST_CONFIG = {
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
            "path": str(MHUB_TEST_BUCKET),
        },
        "pyramid": {"grid": "geodetic", "metatiling": 2},
        "zoom_levels": {"min": 0, "max": 13},
    },
    "dask_specs": {
        "worker_cores": 0.2,
        "worker_cores_limit": 0.3,
        "worker_memory": 1.0,
        "worker_memory_limit": 2.0,
        "worker_threads": 1,
        "scheduler_cores": 1,
        "scheduler_cores_limit": 1.0,
        "scheduler_memory": 1.0,
        "adapt_options": {"minimum": 0, "maximum": 2, "active": "true"},
    },
}
