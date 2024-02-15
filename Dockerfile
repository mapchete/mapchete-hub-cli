ARG BASE_IMAGE=registry.gitlab.eox.at/maps/mapchete_hub/mhub
ARG BASE_IMAGE_TAG=2024.2.5

FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG}

WORKDIR /mnt/data

COPY requirements.txt .
COPY requirements_test.txt .

RUN pip install -r requirements.txt && pip install -r requirements_test.txt

# copy mapchete_hub source code and install
COPY . .
RUN pip install -e .
