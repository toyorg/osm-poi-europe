# POIs for MIB based on OpenStreetMap

## Tested train versions
| Train version         | Compatible |
|-----------------------|------------|
| `MHI2_ER_VWG13_K4525` | Y          |

Should work on other versions

## POI categories
- Fuel stations
- Average speed cameras (start and end)
- Speed cameras
- Speed bumps
- Rail crossings
- Fast food (McDonald's, Burger King, Subway, KFC, Max Premium Burgers)

## Development setup (uv)

This project now uses `uv` for dependency installation.

1. Install `uv` (https://docs.astral.sh/uv/)
2. Create a virtual environment:
  - `uv venv`
3. Install dependencies from `pyproject.toml`:
  - `uv sync`
4. Run scripts inside the environment, for example:
  - `uv run python getdata.py`

### Credits

  - Based on https://github.com/jimmyH/mypois/
  - Data from https://overpass-api.de/
