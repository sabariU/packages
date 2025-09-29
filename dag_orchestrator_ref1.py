# dags/central_etl_orchestrator.py
from __future__ import annotations
from airflow.decorators import dag, task, task_group
import pendulum

SGT = pendulum.timezone("Asia/Singapore")
MODULES: list[str] = ["ichamp", "drone", "bitbucket"]  # add more as needed

@dag(
    dag_id="central_etl_orchestrator",
    start_date=pendulum.datetime(2025, 1, 1, tz=SGT),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,            # one weekly run at a time
    default_args={"owner": "data-eng", "retries": 1}
)
def central_etl_orchestrator():
    @task
    def make_batch_id(data_interval_start=None, run_id: str | None = None) -> str:
        # Stable across retries; easy to query downstream systems by week
        week = pendulum.instance(data_interval_start).format("YYYY-[W]WW")
        return f"{week}-{run_id}"

    batch_id = make_batch_id()

    def module_flow(module: str):
        @task_group(group_id=module)
        def g():
            @task(task_id="extract")
            def extract(batch_id: str, module: str) -> str:
                # call your real extractor here (e.g., ichamp.extract(batch_id))
                return f"{module}:raw:{batch_id}"

            @task(task_id="transform")
            def transform(raw: str, batch_id: str, module: str) -> str:
                # call your real transformer
                return f"{raw}:clean"

            @task(task_id="load")
            def load(ds: str, batch_id: str, module: str) -> None:
                # call your real loader
                print(f"Loaded {module} with {batch_id}: {ds}")

            raw = extract(batch_id, module)
            ready = transform(raw, batch_id, module)
            load(ready, batch_id, module)

        return g()

    groups = [module_flow(m) for m in MODULES]

    @task
    def finalize(batch_id: str):
        print(f"All modules finished for batch {batch_id}")

    groups >> finalize(batch_id)

dag = central_etl_orchestrator()





----

# dags/central_orchestrator.py
from __future__ import annotations
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

SGT = pendulum.timezone("Asia/Singapore")
MODULE_DAGS: list[str] = ["ichamp_etl", "drone_etl", "bitbucket_etl"]

@dag(
    dag_id="central_orchestrator",
    start_date=pendulum.datetime(2025, 1, 1, tz=SGT),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
)
def central_orchestrator():
    @task
    def make_batch_id(data_interval_start=None, run_id: str | None = None) -> str:
        week = pendulum.instance(data_interval_start).format("YYYY-[W]WW")
        return f"{week}-{run_id}"

    bid = make_batch_id()
    for dag_id in MODULE_DAGS:
        TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            conf={"batch_id": bid},
            wait_for_completion=True,   # or False for fire-and-forget
            poke_interval=30,
            reset_dag_run=True,
        )

central = central_orchestrator()
