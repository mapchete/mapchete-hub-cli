import logging
import sys

# lower stream output log level
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
logging.getLogger("mapchete_hub_cli").addHandler(stream_handler)


def set_log_level(loglevel):  # pragma: no cover
    stream_handler.setLevel(loglevel)
    logging.getLogger("mapchete_hub_cli").setLevel(loglevel)
