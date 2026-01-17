from __future__ import annotations

import queue
from argparse import ArgumentParser, Namespace
from pathlib import Path
from time import monotonic as now
from typing import Any
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import osmium
import osmium.filter
import osmium.osm
from geopandas import GeoDataFrame
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.backends.backend_svg import FigureCanvasSVG
from matplotlib.figure import Figure

start = now()


def log(x: Any) -> None:
    t = now() - start
    print(f"{t:.2f}: {x}")


def handle_args() -> Namespace:
    ap = ArgumentParser()
    ap.add_argument("-s", "--source", required=True, help="Path to source data (*.osm.pbf)", type=Path)
    ap.add_argument("-d", "--dest", required=True, help="Path to destination file", type=Path)
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
                # ("natural", "strait"),
                # ("natural", "bay"),
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


class Graph:
    def __init__(self) -> None:
        self.edges: dict[int, list[int]] = defaultdict(list)

    def add(self, a: int, b: int) -> None:
        self._add(a, b)
        self._add(b, a)

    def _add(self, a: int, b: int) -> None:
        self.edges[a].append(b)

    def neighbors(self, a: int) -> list[int]:
        return self.edges[a]

def intersects(idx_a, a, gdf):
    bs = []
    for idx_b, b in gdf.iterrows():
        if a.intersects(b["geometry"]):
            bs.append(idx_b)
    return idx_a, bs


def reachability_filter(gdf: GeoDataFrame) -> GeoDataFrame:
    print(gdf)
    graph = Graph()
    frontier = queue.SimpleQueue()
    futures = []
    with ProcessPoolExecutor(max_workers=16) as pool:
        for idx_a, a in gdf.iterrows():
            a_name = a["name"]
            if a_name == "Lake Washington":
                frontier.put_nowait(idx_a)
            futures.append(pool.submit(intersects, idx_a, a["geometry"], gdf.iloc[(idx_a + 1):]))

        total = len(futures)
        for idx, future in enumerate(futures):
            print(f"{idx} / {total}")
            idx_a, idx_bs = future.result()
            for idx_b in idx_bs:
                graph.add(idx_a, idx_b)

    reached = set()
    while frontier.qsize() > 0:
        node = frontier.get_nowait()
        if node in reached:
            print(len(reached))
            continue
        reached.add(node)
        for neighbor in graph.neighbors(node):
            frontier.put_nowait(neighbor)

    result = gdf.iloc[list(reached)]
    print(result)
    return result



def render(gdf: GeoDataFrame, dest: Path) -> None:
    log("start render")
    kms = 600
    meters = kms * 1000
    pixels_per_meter = 0.01
    size_px = meters * pixels_per_meter
    dpi = 100
    size_in = size_px / dpi
    fig = Figure(figsize=(size_in, size_in), dpi=dpi)
    ax = fig.add_subplot()
    gdf.plot(ax=ax, linewidth=1)
    assert not dest.is_dir()
    # FigureCanvasSVG(fig).print_svg(dest)
    FigureCanvasAgg(fig).print_png(dest)


def main() -> None:
    args = handle_args()
    gdf = read(args.source)
    gdf = reachability_filter(gdf)
    render(gdf, args.dest)
    log("done")


if __name__ == "__main__":
    main()
