from datetime import datetime

import pytest

from mapchete_hub_cli.time import str_to_date


@pytest.mark.parametrize(
    "value",
    [
        "2024-01-01",
        "2024-01-01T00:00:00.000000",
        "2024-01-01 00:00:00.000000",
    ],
)
def test_str_to_date(value):
    assert isinstance(str_to_date(value), datetime)
