#########
Changelog
#########


---------------------
2023.1.0 - 2023-01-24
---------------------

    * CLI
        * add allow submitting `:last:` instead of a job_id so mhub will automatically find the most recently updated job


----------------------
2022.11.0 - 2022-04-01
----------------------

    * CLI
        * add ``--use-old-image`` to ``mhub retry`` to force using the image the job was originally run on; per default the currently deployed image is used
    
    * packaging
        * use `hatch` to build package


---------------------
2022.4.0 - 2022-04-01
---------------------

    * CLI
        * add ``--dask-no-results``: don't let mhub call `Future.results()`
        * add ``mhub --remote-versions``: show remote mhub version
        * add ``--area`` and ``--area-crs`` options: define process area


---------------------
2022.3.0 - 2022-03-14
---------------------

    * CLI
        * add ``--dask-no-task-graph`` flag


----------------------
2021.12.0 - 2021-12-02
----------------------

    * CLI
        * enable passing on dask_max_submitted_tasks and dask_chunksize
        * reintroduce ``progress`` command
        * add ``--show-process`` flag
        * print dask dashboard on default

----------------------
2021.11.0 - 2021-11-05
----------------------

    * CLI
        * add option to show mapchete config
        * add option to pass on custom dask specs from JSON file

----------------------
2021.10.0 - 2021-10-01
----------------------

    * packaging
        * change version numbering scheme to ``YYYY.MM.x``

    * core
        * add environment configuration via MHUB_HOST, MHUB_USER, MHUB_PASSWORD environment variables.

    * CLI
        * smooth progress bar
        * better error handling

    * testing
        * use newest mhub release for testing

----------------
0.2 - 2021-09-23
----------------
    * enable setting dask worker and scheduler specs
    * use black & flake8 for code


----------------
0.1 - 2021-09-22
----------------
    * first version supporting mapchete Hub 0.20