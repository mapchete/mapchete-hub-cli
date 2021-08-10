import logging
import sys

# lower stream output log level
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
logging.getLogger("mapchete_hub_cli").addHandler(stream_handler)


def set_log_level(loglevel):
    stream_handler.setLevel(loglevel)
    logging.getLogger("mapchete_hub_cli").setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(KeyValueFilter(key_value_replace=key_value_replace_patterns))
    logging.getLogger("mapchete_hub_cli").addHandler(file_handler)
    logging.getLogger("mapchete_hub_cli").setLevel(logging.DEBUG)
