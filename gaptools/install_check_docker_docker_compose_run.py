import os
import sys
import subprocess
import platform
from prefect import Flow, task, Parameter, flow

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)
from src.utils import folder_dl, ccdi_wf_outputs_ul


@task
def install_docker():
    system_platform = platform.system().lower()

    if system_platform == "linux":
        subprocess.run(["sudo", "apt-get", "update", "-y"])
        subprocess.run(
            [
                "sudo",
                "apt-get",
                "install",
                "docker-ce",
                "docker-ce-cli",
                "containerd.io",
                "-y",
            ]
        )
    elif system_platform == "darwin":
        subprocess.run(["brew", "install", "docker"])
    elif system_platform == "windows":
        print("Please install Docker Desktop for Windows manually.")
        print("https://docs.docker.com/desktop/install/windows-install/")
    else:
        print("Unsupported platform.")


@task
def install_docker_compose():
    subprocess.run(
        [
            "sudo",
            "curl",
            "-L",
            "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)",
            "-o",
            "/usr/local/bin/docker-compose",
        ]
    )
    subprocess.run(["sudo", "chmod", "+x", "/usr/local/bin/docker-compose"])


@task
def check_docker_installed():
    result = subprocess.run(
        ["docker", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return result.returncode == 0, result.stdout.decode()


@flow(log_prints=True, flow_run_name="run-gaptool")
def run_gaptool():
    install_docker()
    install_docker_compose()

    folder_dl(bucket="ccdi-validation", remote_folder="QL/dbgap_input_output")
    subprocess.run(["mkdir", "QL/dbgap_input_output/outputs/QL_test_2024-01-31"])

    os.chdir("gaptools")
    subprocess.run(
        [
            "./dbgap-docker.bash",
            "-i",
            "../QL/dbgap_input_output/inputs/QL_test_dbGaP_submission_2023-12-21",
            "-o",
            "../QL/dbgap_input_output/outputs/QL_test_2024-01-31",
            "-m",
            "../QL/dbgap_input_output/inputs/QL_test_dbGaP_submission_2023-12-21/metadata.json",
        ]
    )

    result = subprocess.run(
        ["ls", "-ll", "../QL/dbgap_input_output/outputs/QL_test_2024-01-31"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(result.stdout.decode())

