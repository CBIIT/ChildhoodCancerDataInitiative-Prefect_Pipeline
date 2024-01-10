from prefect import flow, task


@flow(
    name="variable inputs test",
    log_prints=True,
)
def pass_variable_args(*buckets):
    for i in buckets:
        print(i)
    print("Hello World")

if __name__ == "__main__":
    mylist= ["apple", "orange", "pear", "grape", "melon"]
    pass_variable_args(*mylist)