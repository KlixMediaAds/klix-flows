from prefect import flow, task

@task
def say(msg: str): print(msg)

@flow
def hello(msg: str = "Hello from Prefect Managed!"):
    say(msg)

if __name__ == "__main__":
    hello()
