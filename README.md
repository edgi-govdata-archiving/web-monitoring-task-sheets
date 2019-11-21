# Web Monitoring Task Sheets

**⚠️ This is a work in progress, and is pretty messy! There will be lots of commits directly on `master` for now. ⚠️**

This project is a re-envisioning of our process for generating weekly analyst tasking spreadsheets. It pulls down information from a web-monitoring-db instance about all the changes that have occurred over a given timeframe and then attempts to analyze them and assign a meaningful priority to each page.

Run it like:

```sh
# Generate task sheets covering the timeframe between November 10, 2019 and now
# and save them in the ./task-sheets directory.
> python generate_task_sheets.py --after '2019-11-10T00:00:00Z' --output ./task-sheets
```

There are a slew of other options you can find out about with the `--help` option:

```sh
> python generate_task_sheets.py --help
```

It requires a copy of `readability-server` from [web-monitoring-changed-terms-analysis](https://github.com/edgi-govdata-archiving/web-monitoring-changed-terms-analysis) to be running.

The actual analysis routines can be found in [`./analyst_sheets/analyze.py`](./analyst_sheets/analyze.py).


## Installation

1. Install Python

2. Clone this repo:

    ```sh
    > git clone xyz
    ```

3. Make a virtualenv and install python dependencies:

    ```sh
    > cd xyz
    > pyenv virtualenv 3.7.4 wm-task-sheets
    > pyenv activate wm-task-sheets
    > pip install --upgrade pip  # Make sure pip is up-to-date
    > pip install -r requirements.txt
    ```

4. Run it!

    ```sh
    > python generate_task_sheets.py --help
    ```


## License & Copyright

Copyright (C) 2019 Environmental Data and Governance Initiative (EDGI)
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.0.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the [`LICENSE`](/LICENSE) file for details.
