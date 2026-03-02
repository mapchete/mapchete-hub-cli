#########
Changelog
#########

---------------------
2026.3.0 - 2026-03-02
---------------------

* core

  * add add "Content-Type" header when sending a POST request (#22)

* CI

  * use `ghcr.io/mapchete/mapchete:2026.2.2` for testing (#22)


---------------------
2025.9.0 - 2025-09-02
---------------------

* packaging

  * add `click-plugins` as dependency (#5)
  * add `uv.lock` file (#5)


* CI

  * use new `mapchete/mapchete` image for testing (#5)
  * add python `3.13` to test matrix (#5)


---------------------
2025.8.0 - 2025-08-06
---------------------

package migrated to GitHub


----------------------
2024.12.0 - 2024-12-13
----------------------

    * core

        * `execute`: fix issue where providing bounds and area, zones would be all zones intersecting with area bounds


----------------------
2024.10.1 - 2024-10-11
----------------------

    * core

        * add `--count` and `--wait-time` options to `test-run` subcommand
        * make sure every generated timestamp for here and now has a localized timezone and can optionally be converted to UTC


----------------------
2024.10.0 - 2024-10-02
----------------------

    * core

        * add `test-run` subcommand
        * `execute`: add `--zones-within-area` flag
        * add `show-remote-version` subcommand to replace `--show-remote-version` flag
  
    * packaging

      * switch pre-commit tools to `ruff` and `mypy`
      
    * CI

      * add codecheck stage


---------------------
2024.4.1 - 2024-04-15
---------------------

    * core

        * `execute`: add `--unique-by-job-name` flag

---------------------
2024.4.0 - 2024-04-12
---------------------

    * core

        * add `--area` to `mhub execute``


---------------------
2024.2.2 - 2024-02-15
---------------------

    * core

        * better messages: ask before submitting a bunch of jobs & also print job names


---------------------
2024.2.1 - 2024-02-06
---------------------

    * core

        * restructured large parts of package
        * added typing


---------------------
2024.2.0 - 2024-02-06
---------------------

    * core

        * add `--full-zones` to `mhub execute``
        * add `mhub clean` subcommand


----------------------
2023.12.1 - 2023-12-12
----------------------

    * core

        * add `--make-zones-on-zoom` and `--zone` options to `mhub execute``


----------------------
2023.12.0 - 2023-12-11
----------------------

    * core

        * adapt CLI to breaking changes made in `mapchete_hub` `2023.12.0`

    * CI

        * deactivate integration tests, because they are currently not working!


---------------------
2023.6.0 - 2023-06-13
---------------------

    * core
        * add `initializing` and `created` Job states available via `mapchete_hub>=2023.6.4`


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