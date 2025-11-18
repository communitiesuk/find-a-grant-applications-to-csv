# Find A Grant Submissions API to CSV converter


## Prerequisites
* `python3` on path
* `uv`
* A GGIS reference number for a grant
* An API key and hostname for the Cabinet Office Find A Grant API

## Setup

Sync dependencies and create a virtual environment:
```
uv venv
uv sync
```

## How to run the script

Run the script using uv run (no need to activate the venv):

```
uv run python applications_to_csv.py --api-base <URL> --ggis-reference-number <GGIS_REFERENCE_NUMBER> --api-key <API>
```

## How to run tests
```
python3 -m unittest
```
