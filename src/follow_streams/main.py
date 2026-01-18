from __future__ import annotations

import multiprocessing as mp
import queue
from argparse import ArgumentParser, Namespace
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from time import monotonic as now
from typing import Any

import osmium
import osmium.filter
import osmium.osm
from geopandas import GeoDataFrame
from matplotlib.backends.backend_agg import FigureCanvasAgg
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


class DropIntermittentFilter:
    def __init__(self) -> None:
        self.node = self._should_drop
        self.way = self._should_drop
        self.relation = self._should_drop

    def _should_drop(self, v: Any) -> bool:
        # A filter is a handler that returns a boolean in the handler
        # functions indicating if the object should pass the filter (False)
        # or be dropped (True).
        return v.tags.get("intermittent") == "yes"


class DropIdFilter:
    def __init__(self, ident: int) -> None:
        self._ident = ident

    def way(self, v: Any) -> bool:
        return v.id == self._ident


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
                ("natural", "strait"),
                ("natural", "bay"),
                ("water", "lake"),
                # ("water", "oxbow"),
                ("water", "river"),
                ("water", "stream"),
                # ("water", "pond"),
                ("water", "reservoir"),
                ("waterway", "stream"),
                ("waterway", "river"),
                ("waterway", "tidal_channel"),
                ("waterway", "canal"),
                # ("waterway", "ditch"),
                # ("waterway", "drain"),
            )
        )
        .with_filter(DropIntermittentFilter())
        .with_filter(DropIdFilter(631469130))  # Stream goes uphill here! 47.02694289590613, -122.93298280193007
        .with_filter(osmium.filter.GeoInterfaceFilter(tags=["name"]))
    )
    return GeoDataFrame.from_features(fp, crs="EPSG:4326")


# depends on fork!
g_frontier = mp.Queue()
g_gdf: None | GeoDataFrame = None


def find_neighbors(start_idx: Any) -> None:
    global g_frontier
    global g_gdf
    assert g_gdf is not None
    start_geo = g_gdf.loc[start_idx]["geometry"]
    is_touching = g_gdf["geometry"].intersects(start_geo)
    for idx in g_gdf[is_touching].index:
        g_frontier.put_nowait(idx)


def reachability_filter(gdf: GeoDataFrame) -> GeoDataFrame:
    global g_gdf

    # Quick filter based on rectangle that puget sound watershed is in
    centroids = gdf["geometry"].centroid
    close_enough = (centroids.x < -120.6) & (centroids.y > 46.5)
    gdf = gdf[close_enough]
    print(gdf)

    # Expensive filter by graph reachability
    puget_sound_idx = gdf[gdf["name"] == "Puget Sound"].index[0]
    g_frontier.put_nowait(puget_sound_idx)
    reached = set()
    iters = 0
    in_prog_futures = set()
    g_gdf = gdf
    assert mp.get_start_method() == "fork"
    with ProcessPoolExecutor() as pool:
        while g_frontier.qsize() > 0 or len(in_prog_futures) > 0:
            if iters % 1000 == 0:
                log(f"{iters=}, frontier={g_frontier.qsize()}, reached={len(reached)}")
            iters += 1

            try:
                node = g_frontier.get(timeout=0.1)
            except queue.Empty:
                # if there's no work to do for this process. Wait for a result from another process.
                done_futures, in_prog_futures = wait(in_prog_futures, return_when=FIRST_COMPLETED)
                for f in done_futures:
                    f.result()  # raise exceptions
                continue
            if node in reached:
                continue
            reached.add(node)
            in_prog_futures.add(pool.submit(find_neighbors, node))
    gdf = gdf.loc[list(reached)]
    print(gdf)

    return gdf


def render(gdf: GeoDataFrame, dest: Path) -> None:
    log("start render")
    width_px = 2000
    dpi = 100
    width_inch = width_px / dpi
    height_inch = 1.2 * width_inch
    fig = Figure(figsize=(width_inch, height_inch), dpi=dpi, layout="constrained")
    ax = fig.add_subplot()
    gdf.plot(ax=ax, linewidth=1)
    ax.set_axis_off()
    assert not dest.is_dir()
    FigureCanvasAgg(fig).print_png(dest)


def main() -> None:
    args = handle_args()
    here = Path(__file__).resolve().parent
    cache_dir = here / ".gdf_cache"
    if cache_dir.exists():
        gdf = GeoDataFrame.from_file(cache_dir)
    else:
        gdf = read(args.source)
        gdf = reachability_filter(gdf)
        gdf.to_file(cache_dir)
    render(gdf, args.dest)
    log("done")


if __name__ == "__main__":
    main()
