FROM prefecthq/prefect:2.13.0-python3.11
COPY requirements.txt /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/requirements.txt
RUN python -m pip install -r /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/requirements.txt
COPY . /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/
WORKDIR /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/
