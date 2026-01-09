# rt_notebook

Marimo notebook for looking at MBTA GTFS RT feeds via LAMP catalog. This allows for playback of all modes across the dev-green and prod environments. 


You need accesss to the MBTA's AWS S3 buckets to use this tool.

## Setup

### Prerequisites:
- [mise](https://mise.jdx.dev/) (optional)
- [uv](https://docs.astral.sh/uv/)


First, get `uv`. If you're using `mise` you should be able to just run `mise install`. Then run `uv run marimo edit notebook.py`. `uv` should take care of installing the dependencies

For accessing LAMP, you will need AWS credentials set up locally.

If you just want to run the map, click "run all cells" on the hamburger menu at the top.


[Demo video](demo.mp4)
