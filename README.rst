mapchete Hub CLI
================

A command line interface for mapchete Hub.

.. image:: https://img.shields.io/pypi/v/mapchete-hub-cli.svg
  :target: https://pypi.org/project/mapchete-hub-cli/

.. image:: https://img.shields.io/pypi/l/mapchete-hub-cli.svg
  :target: https://github.com/mapchete/mapchete-hub-cli/blob/main/LICENSE

.. image:: https://img.shields.io/github/actions/workflow/status/mapchete/mapchete-hub-cli/python-package.yml?label=tests
  :target: https://github.com/mapchete/mapchete-hub-cli/actions

.. image:: https://codecov.io/gh/mapchete/mapchete-hub-cli/graph/badge.svg?token=VD1YOF3QA2
  :target: https://codecov.io/gh/mapchete/mapchete-hub-cli

.. image:: https://img.shields.io/github/repo-size/mapchete/mapchete-hub-cli
  :target: https://github.com/mapchete/mapchete-hub-cli

About mapchete Hub CLI
========================

The mapchete Hub CLI provides a user-friendly command line interface (``mhub``) to interact with a running mapchete Hub instance. It simplifies the process of submitting, monitoring, and managing processing jobs without needing to write custom scripts or use tools like `curl`.

This CLI is the recommended tool for users who want to script workflows or manage mapchete Hub jobs from their terminal.


Key Features
============

* **💻 Full Job Lifecycle Management**: Intuitive commands to ``execute``, ``job`` (status), ``jobs`` (list), ``cancel``, and ``retry`` processing jobs.
* **⚙️ Hub Inspection**: List available ``processes`` on a remote mapchete Hub and check remote package ``--remote-versions``.
* **🔒 Authentication Support**: Handles basic authentication via command line options (``-u``, ``-p``) or environment variables.
* **🤖 Script-Friendly**: Designed for easy integration into automated scripts and workflows.


Installation
============

Install the CLI and its dependencies from PyPI:

.. code-block:: bash

   pip install mapchete-hub-cli


Quickstart
==========

**1. Point to your mapchete Hub**

All commands require you to specify the mapchete Hub's URL using the ``-h`` or ``--host`` option. If the mapchete Hub is running on the same machine with default settings (``0.0.0.0:5000``), you may not need this option.

**2. List Available Processes**

You can see what processes are available on the target mapchete Hub instance:

.. code-block:: bash

   mhub -h 0.0.0.0:5000 processes

**3. Prepare a mapchete File**

Create a `mapchete process file <https://github.com/mapchete/mapchete>`_ using YAML syntax. **Note that all input and output paths must be absolute**, as they are resolved on the remote server.

For example, `hillshade.mapchete`:

.. code-block:: yaml

   process: mapchete.processes.examples.hillshade
   zoom_levels:
     - 10
   pyramid:
     grid: geodetic
   input:
     dem: https://data.example.com/path/to/dem.tif
   output:
     path: s3://my-bucket/path/to/hillshade_output
     format: GTiff

**4. Execute the Process**

Use the ``execute`` command to send the job to the mapchete Hub.

.. code-block:: bash

   mhub -h 0.0.0.0:5000 execute hillshade.mapchete

The mapchete Hub will return a unique `job_id`:

.. code-block:: text

   6227b68b-b7e6-4f40-a396-85763197f481

**5. Check Job Status and Progress**

Use the `job_id` with the ``job`` command to get a detailed status report, or ``progress`` for a quick look at the progress percentage.

.. code-block:: bash

   # Get detailed status
   mhub -h 0.0.0.0:5000 job 6227b68b-b7e6-4f40-a396-85763197f481

   # Get just the progress
   mhub -h 0.0.0.0:5000 progress 6227b68b-b7e6-4f40-a396-85763197f481

**6. Manage Jobs**

You can also list all jobs or cancel a running job:

.. code-block:: bash

   # List all jobs on the mapchete Hub
   mhub -h 0.0.0.0:5000 jobs

   # Cancel a specific job
   mhub -h 0.0.0.0:5000 cancel 6227b68b-b7e6-4f40-a396-85763197f481


Contributing
============

This is an open-source project and we welcome contributions! Please see the `Contributing Guide <https://github.com/mapchete/mapchete/blob/main/CONTRIBUTING.md>`_ in the main ``mapchete`` repository for guidelines on how to get started.

Acknowledgements
================

The initial development of this project was made possible with the resources and support of `EOX IT Services GmbH <https://eox.at/>`_.