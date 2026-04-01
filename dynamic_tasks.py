from airflow.sdk import dag, task

REGIONS = [
    {"region": "north", "target": 50_000},
    {"region": "south", "target": 38_000},
    {"region": "east",  "target": 62_000},
    {"region": "west",  "target": 44_000},
]


def _fetch(region: str) -> list[dict]:
    """Simulate pulling sales rows for a region."""
    import random
    random.seed(region)
    return [
        {"product": f"Widget {i}", "amount": round(random.uniform(100, 999), 2)}
        for i in range(1, 6)
    ]


@dag
def dynamic_tasks():

    # ------------------------------------------------------------------
    # 1. Emit the list of work items that will be mapped over.
    #    Each dict becomes the kwargs for one mapped instance downstream.
    # ------------------------------------------------------------------
    @task
    def get_regions() -> list[dict]:
        return REGIONS

    # ------------------------------------------------------------------
    # 2. fetch_sales — dynamically mapped over every region dict.
    #
    #    map_index_template controls the label shown in the Airflow UI
    #    for each instance. The template is a Jinja expression evaluated
    #    against the task context; {{ task.op_kwargs }} exposes the kwargs
    #    passed to that specific instance, so we can surface the region
    #    name rather than a raw integer index.
    #
    #    Without map_index_template the Grid view shows:
    #        fetch_sales[0], fetch_sales[1], ...
    #
    #    With it:
    #        fetch_sales[north], fetch_sales[south], ...
    # ------------------------------------------------------------------
    @task(map_index_template="{{ task.op_kwargs['region_cfg']['region'] }}")
    def fetch_sales(region_cfg: dict) -> dict:

        region  = region_cfg["region"]
        target  = region_cfg["target"]
        rows    = _fetch(region)
        total   = round(sum(r["amount"] for r in rows), 2)
        pct     = round(total / target * 100, 1)
        print(f"[{region}] total={total}  target={target}  attainment={pct}%")

        return {"region": region, "total": total, "target": target, "attainment_pct": pct}

    # ------------------------------------------------------------------
    # 3. flag_underperformers — also dynamically mapped, receives the
    #    output of each fetch_sales instance 1-to-1 (partial mapping).
    #    map_index_template re-uses the region from the incoming payload.
    # ------------------------------------------------------------------
    @task(map_index_template="{{ task.op_kwargs['summary']['region'] }}")
    def flag_underperformers(summary: dict) -> None:
        if summary["attainment_pct"] < 80:
            print(f"ALERT  {summary['region']} at {summary['attainment_pct']}% of target")
        else:
            print(f"OK     {summary['region']} at {summary['attainment_pct']}% of target")

    # ------------------------------------------------------------------
    # 4. collect_results — reduces all mapped outputs into one task.
    #    Receives a list[dict] automatically assembled from every
    #    fetch_sales instance.
    # ------------------------------------------------------------------
    @task
    def collect_results(summaries: list[dict]) -> None:
        print("\n── Regional summary ──────────────────────")
        for s in sorted(summaries, key=lambda x: x["attainment_pct"], reverse=True):
            bar = "█" * int(s["attainment_pct"] / 10)
            print(f"  {s['region']:<6} {bar:<10} {s['attainment_pct']}%")
        print("──────────────────────────────────────────")

    # ------------------------------------------------------------------
    # Wiring
    # expand() fans out over the XComArg returned by get_regions(), creating
    # one mapped task instance per element. Each element is passed as the
    # key word argument — one dict, one instance.
    # ------------------------------------------------------------------
    regions  = get_regions()
    summaries = fetch_sales.expand(region_cfg=regions)
    flag_underperformers.expand(summary=summaries)
    collect_results(summaries)

dynamic_tasks()