from prefect import flow, task

@task
def greeting(name):
    print(f"Hello {name}!")

@flow(log_prints = True)
"""
A simple script that prints "Hello" followed by a user-specified name.
Args:
    name (str): the user-specified name as a string, ie "Jane Doe". 
    Defaults to "World".
""" 
def hello_flow(name: str = "World"):
    greeting(name)

if __name__ == "__main__":
    hello_flow()