import math
from copy import deepcopy
from argparse import ArgumentParser, Namespace
from geopandas import GeoDataFrame
from joblib import Memory
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from pathlib import Path
from pyrosm import OSM
from time import monotonic as now
from typing import Any
import json
import pandas as pd
import shapely

HERE = Path(__file__).resolve().parent
memory = Memory(HERE / ".joblib_cache", verbose=2)
start = now()

def log(x: Any) -> None:
    t = now() - start
    print(f"{t:.2f}: {x}")


def handle_args() -> Namespace:
    ap = ArgumentParser()
    ap.add_argument("-s", "--source", help="Path to source data (*.osm.pbf)", type=Path)
    ap.add_argument("-d", "--dest", help="Path to destination file (*.png)", type=Path)
    return ap.parse_args()
    
@memory.cache
def read(source: Path) -> GeoDataFrame:
    log(f"start reading from {source}")
    osm = OSM(str(source))
    result = osm.get_natural(
        custom_filter={
            "natural": ["water"],
        },
    )
    log(f"done reading from {source}")
    return result

type Coord = tuple[float, float]

def dist(a: Coord, b: Coord) -> float:
    return math.sqrt((a[0] - b[0])**2 + (a[1] - b[1])**2)

class Candidate:
    def __init__(self, idx: int, coords: list[Coord]) -> None:
        self.idx = idx
        self.coords = coords

    def dist(self, a: Coord) -> float:
        dist_to_start = dist(a, self.coords[0])
        dist_to_end = dist(a, self.coords[-1])
        return min(dist_to_start, dist_to_end)

    def get(self, a: Coord | None) -> list[Coord]:
        if a is None:
            return self.coords
        dist_to_start = dist(a, self.coords[0])
        dist_to_end = dist(a, self.coords[-1])
        if dist_to_end < dist_to_start:
            return list(reversed(self.coords))
        else:
            return self.coords


def flatten(mp: shapely.MultiPolygon) -> shapely.Polygon:
    DEBUG = True
    if DEBUG:
        result = []
        for i, polygon in enumerate(mp.geoms):
            result.extend(list(polygon.exterior.coords))
        return shapely.Polygon(result)
    else:
        candidates = {}
        for i, polygon in enumerate(mp.geoms):
            candidates[i] = Candidate(i, list(polygon.exterior.coords))
        result = []
        start = candidates.pop(0)
        result.extend(start.get(None))
        while len(candidates) > 0:
            target = result[-1]
            closest = min(candidates.values(), key=lambda c: c.dist(target))
            result.extend(closest.get(target))
            log(closest.idx)
            del candidates[closest.idx]
    return shapely.Polygon(result)

def cleanup(geo_data: GeoDataFrame) -> GeoDataFrame:
    log("start filtering geo_data")
    keep = []
    for idx, row in geo_data.iterrows():
        tags = json.loads(row["tags"])
        name = tags.get("name")
        geo = row["geometry"]
        water_type = row["water"]
        if name is not None and "Samm" in name:
            log(f"{name=} has {shapely.get_num_coordinates(geo)} points")
            new_row = deepcopy(row)
            if isinstance(geo, shapely.MultiPolygon):
                new_row["geometry"] = flatten(geo)
            keep.append(new_row)
    log("done filtering geo_data")
    return GeoDataFrame(keep, crs=geo_data.crs)

def render(geo_data: GeoDataFrame, dest: Path) -> None:
    log("start render")
    fig = Figure()
    ax = fig.add_subplot()
    for idx, row in geo_data.iterrows():
        print(row)
    geo_data.plot(ax=ax)
    canvas = FigureCanvas(fig)
    assert not dest.is_dir()
    assert dest.suffix == ".png"
    canvas.print_png(dest)
    log("done render")


def main() -> None:
    args = handle_args()
    geo_data = read(args.source)
    geo_data = cleanup(geo_data)
    render(geo_data, args.dest)

if __name__ == "__main__":
    main()