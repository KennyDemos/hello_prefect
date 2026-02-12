from prefect import flow, task
import time

@task(retries=2, retry_delay_seconds=2)
def step(name: str):
    print(f"Running step: {name}")
    time.sleep(1)
    return name.upper()

@flow(log_prints=True)
def hello_flow(who: str = "Kenny"):
    a = step("download")
    b = step("transform")
    c = step("load")
    return f"Done for {who}: {a}, {b}, {c}"

if __name__ == "__main__":
    print(hello_flow())

