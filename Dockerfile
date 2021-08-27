FROM registry.gitlab.eox.at/maps/mapchete_hub/mhub:fastapi_dask
MAINTAINER Joachim Ungar

WORKDIR /mnt/data

COPY requirements.txt .
COPY requirements_test.txt .

RUN pip install -r requirements.txt && pip install -r requirements_test.txt

# copy mapchete_hub source code and install
COPY . .
RUN pip install -e .
