from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from time import monotonic as now
from typing import Any

import osmium
import osmium.filter
import osmium.osm
from geopandas import GeoDataFrame
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

start = now()


def log(x: Any) -> None:
    t = now() - start
    print(f"{t:.2f}: {x}")


def handle_args() -> Namespace:
    ap = ArgumentParser()
    ap.add_argument("-s", "--source", required=True, help="Path to source data (*.osm.pbf)", type=Path)
    ap.add_argument("-d", "--dest", required=True, help="Path to destination file (*.png)", type=Path)
    return ap.parse_args()


def read(source: Path) -> GeoDataFrame:
    log("start read")
    fp = (
        osmium.FileProcessor(str(source))
        .with_locations()
        .with_areas()
        .with_filter(osmium.filter.EntityFilter(osmium.osm.AREA | osmium.osm.WAY))
        .with_filter(
            osmium.filter.TagFilter(
                ("natural", "water"),
                ("water", "lake"),
                ("water", "oxbow"),
                ("water", "river"),
                ("water", "stream"),
                ("water", "pond"),
                ("water", "reservoir"),
                ("waterway", "stream"),
                ("waterway", "river"),
                ("waterway", "tidal_channel"),
                ("waterway", "canal"),
                ("waterway", "ditch"),
                ("waterway", "drain"),
            )
        )
        .with_filter(osmium.filter.GeoInterfaceFilter(tags=["name"]))
    )
    return GeoDataFrame.from_features(fp, crs="EPSG:4326")


def render(gdf: GeoDataFrame, dest: Path) -> None:
    log("start render")
    fig = Figure(figsize=(5, 5), dpi=400)
    ax = fig.add_subplot()
    gdf.plot(ax=ax)
    assert not dest.is_dir()
    assert dest.suffix == ".png"
    FigureCanvas(fig).print_png(dest)


def main() -> None:
    args = handle_args()
    gdf = read(args.source)
    render(gdf, args.dest)


if __name__ == "__main__":
    main()
