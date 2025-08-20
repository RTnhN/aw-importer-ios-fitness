aw-importer-ios-fitness
==================

This extension imports fitness data from [csv health export](https://apps.apple.com/us/app/simple-health-export-csv/id1535380115) by watching a folder for changes.


This watcher is currently in a early stage of development, please submit PRs if you find bugs!


## Usage

### Step 1: Install package

Install the requirements:

```sh
pip install .
```

or using [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv tool install .
```

This will install the watcher to the USER/.local/bin folder. If this is the first time that you have
installed with uv, you might need to add the folder to your PATH environment variable.


First run (generates empty config that you need to fill out):
```sh
python aw-importer-ios-fitness/main.py
```

First run with uv:
```sh
uv run python aw-importer-ios-fitness/main.py
```

### Step 2: Enter config

You will need to add the path to the folder where you will add the csv files from csv health export.

### Step 3: Add the csv export to the folder

### Step 4: Restart the server and enable the watcher

Reminder: Don't forget to add the watcher to your autostart file.