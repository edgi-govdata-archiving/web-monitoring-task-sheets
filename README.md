# Web Monitoring Task Sheets

**⚠️ This is a messy work in progress! There will be lots of commits directly on `main` for now. ⚠️**

This project is a re-envisioning of our process for generating weekly analyst tasking spreadsheets. It pulls down information from a web-monitoring-db instance about all the changes that have occurred over a given timeframe and then attempts to analyze them and assign a meaningful priority to each page.

Run it like:

```sh
# Generate task sheets covering the timeframe between November 10, 2019 and now
# and save them in the ./task-sheets directory.
> python generate_task_sheets.py --after '2019-11-10T00:00:00Z' --skip-readability --output ./task-sheets
```

There are a slew of other options you can find out about with the `--help` option:

```sh
> python generate_task_sheets.py --help
```

The actual analysis routines can be found in [`./analyst_sheets/analyze.py`](./analyst_sheets/analyze.py).

---

In current production usage, we use [Mozilla’s “Readability” tool](https://github.com/mozilla/readability) (what generates the reader view in Firefox) for some parts of the analysis. It has some issues, though, so there is a partially built alternative/fallback for it in `analyst_sheets/normalize.py:get_main_content`. It’s likely too simplistic to work for a lot of potential cases, though.

- To use Readability, you’ll need Node.js v20 or later installed. Before running `generate_task_sheets.py`, start the Readability server:

    ```sh
    > cd readability-server
    > npm install
    > npm start
    ```

    Then, in a different shell session, run `generate_task_sheets.py` with whatever arguments you want. Afterward, you can shut down the Readability server.

- To use the in-development fallback, specify the `--skip-readability` option when running `generate_task_sheets.py` instead of starting the Readability server.


## Installation

1. Install Python

2. Clone this repo:

    ```sh
    > git clone xyz
    ```

3. Make a virtualenv and install python dependencies:

    ```sh
    > cd xyz
    > pyenv virtualenv 3.10.16 wm-task-sheets
    > pyenv activate wm-task-sheets
    > pip install --upgrade pip  # Make sure pip is up-to-date
    > pip install -r requirements.txt
    ```

4. Run it!

    ```sh
    > python generate_task_sheets.py --help
    ```


## License & Copyright

Copyright (C) 2019–2025 Environmental Data and Governance Initiative (EDGI)
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.0.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the [`LICENSE`](/LICENSE) file for details.
