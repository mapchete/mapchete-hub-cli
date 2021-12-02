#########
Changelog
#########

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