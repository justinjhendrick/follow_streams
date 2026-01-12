from __future__ import annotations

from dataclasses import dataclass
import math
from copy import deepcopy
from argparse import ArgumentParser, Namespace
from joblib import Memory
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from pathlib import Path
from pyrosm.pbfreader import parse_osm_data
from pyrosm.data_manager import get_osm_data
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
    ap.add_argument("-s", "--source", required=True, help="Path to source data (*.osm.pbf)", type=Path)
    ap.add_argument("-d", "--dest", required=True, help="Path to destination file (*.png)", type=Path)
    return ap.parse_args()

type Coord = tuple[float, float]
type Tags = dict[str, str]

@dataclass
class Way:
    ident: int
    tags: Tags
    coords: list[Coord]

@dataclass
class Relation:
    ident: int
    tags: Tags
    way_ids: list[int]  # actually ndarray thing?

type Ways = dict[int, Way]
type Relations = dict[int, Relation]

# @memory.cache
def read(source: Path) -> tuple[Ways, Relations]:
    log(f"start reading from {source}")
    _nodes, ways, relations, node_coords_lookup = parse_osm_data(str(source), None, False, None)
    ways = pd.DataFrame(ways)
    relations = pd.DataFrame(relations)

    my_relations = {}
    for idx, relation in relations.iterrows():
        tags = relation["tags"]
        if tags.get("natural") != "water":
            continue
        if tags.get("name") != "Lake Sammamish":
            continue
        way_ids = relation["members"]["member_id"]
        r = Relation(relation["id"], tags, way_ids)
        my_relations[r.ident] = r

    my_ways = {}
    for idx, way in ways.iterrows():
        ident = way["id"]
        tags = relation["tags"]
        # if tags.get("natural") != "water" and not any(ident in r.way_ids for r in relations):
        if not any(ident in r.way_ids for r in relations):
            continue
        coords = []
        for node_ident in way["nodes"]:
            node = node_coords_lookup[node_ident]
            coords.append((node["lat"], node["lon"]))
        w = Way(ident, tags, coords)
        my_ways[w.ident] = w

    return my_ways, my_relations

def render(ways: Ways, relations: Relations, dest: Path) -> None:
    log("start render")
    fig = Figure()
    ax = fig.add_subplot()
    # TODO
    canvas = FigureCanvas(fig)
    assert not dest.is_dir()
    assert dest.suffix == ".png"
    canvas.print_png(dest)

def main() -> None:
    args = handle_args()
    ways, relations = read(args.source)
    render(ways, relations, args.dest)

if __name__ == "__main__":
    main()
