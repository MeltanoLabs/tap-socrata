# tap-socrata

`tap-socrata` is a Singer tap for [Socrata](https://www.tylertech.com/products/socrata) open data
portals (e.g. `data.cityofnewyork.us`, `data.cityofchicago.org`).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

Install from PyPI:

```bash
uv tool install meltanolabs-tap-socrata
```

Install from GitHub:

```bash
uv tool install git+https://github.com/MeltanoLabs/tap-socrata.git@main
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-socrata --about
```

| Setting | Required | Description |
| --- | --- | --- |
| `domains` | Yes | Domain names to query (e.g. `["data.cityofnewyork.us"]`) |
| `dataset_ids` | No | Restrict discovery to specific Socrata dataset (4x4) IDs, instead of every dataset on `domains` |
| `api_key_id` / `api_key_secret` | No | Socrata API key credentials, for authenticated/private datasets |
| `app_token` / `secret_token` | No | Socrata app token, for higher rate limits |
| `user_agent` | No | Custom User-Agent string to send with requests |

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Public Socrata datasets (like most of [NYC Open Data](https://data.cityofnewyork.us)) don't require
credentials. Registering an `app_token` is recommended to raise the anonymous rate limit. For
private or write-restricted datasets, set `api_key_id`/`api_key_secret`.

## Usage

You can easily run `tap-socrata` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-socrata --version
tap-socrata --help
tap-socrata --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

The standard test suite in `tests/test_core.py` runs a live discovery and sync against a small,
public NYC Open Data dataset (`data.cityofnewyork.us`, Queens Library Branches), so no credentials
are needed. See `.env` for the config used and how to point the tap at other domains/datasets.

You can also test the `tap-socrata` CLI interface directly using `uv run`:

```bash
uv run tap-socrata --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Use Meltano to run an EL pipeline:

```bash
# Install meltano
uv tool install meltano

# Test invocation
meltano invoke tap-socrata --version

# Run a test EL pipeline
meltano run tap-socrata target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
