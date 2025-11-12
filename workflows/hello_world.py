from prefect import flow, task

@task
def greeting(name):
    print(f"Hello {name}!")

@flow(name="hello_flow", log_prints=True)
def hello_flow(name: str):
    """
A simple script that prints "Hello" followed by a user-specified name.
Args:
    name (str): the user-specified name as a string, ie "Jane Doe". 
    Defaults to "World".
    """ 
    greeting(name)

if __name__ == "__main__":
    hello_flow()