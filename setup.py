"""Install Mapchete."""

from setuptools import setup, find_namespace_packages

# get version number
# from https://github.com/mapbox/rasterio/blob/master/setup.py#L55
with open("src/mapchete_hub_cli/__init__.py") as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break


install_requires = [
    "click",
    "oyaml",
    "requests",
    "tqdm",
]
test_requires = [
    "pytest",
    "pytest-cov",
    "pytest-env",
    "mapchete_hub",
]

setup(
    name="mapchete_hub_cli",
    version=version,
    description="CLI and Python bindings to mapchete Hub API.",
    author="Joachim Ungar",
    author_email="joachim.ungar@eox.at",
    url="https://gitlab.eox.at/maps/mapchete_hub_cli",
    license="MIT",
    packages=find_namespace_packages(where="src"),
    package_dir={
        "": "src",
    },
    entry_points={
        "console_scripts": ["mhub=mapchete_hub_cli.cli:mhub"],
        "mapchete.cli.commands": ["mhub=mapchete_hub_cli.cli:mhub"],
    },
    install_requires=install_requires,
    extras_require={"test": test_requires},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
