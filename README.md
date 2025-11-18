# Find A Grant Submissions API to CSV converter


## Prerequisites
* `python3` on path
* [uv](https://github.com/astral-sh/uv) (for fast virtualenv and dependency management)
* A GGIS reference number for a grant
* An API key for the Cabinet Office Find A Grant API


## Setup (recommended: using [uv](https://github.com/astral-sh/uv) and pyproject.toml)

1. Install [uv](https://github.com/astral-sh/uv) if you don't have it:
	```sh
	curl -LsSf https://astral.sh/uv/install.sh | sh
	```

2. Sync dependencies and create a virtual environment:
	```sh
	uv venv
	uv sync
	```

3. Run the script using uv run (no need to activate the venv):
	```sh
	uv run python applications_to_csv.py --api-base <URL> --ggis-reference-number <GGIS_REFERENCE_NUMBER> --api-key <API>
	```

### How to run tests
```
python3 -m unittest
```