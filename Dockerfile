COPY requirements.txt /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/requirements.txt
RUN python -m pip install -r /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/requirements.txt
VOLUME ["/var/run/docker.sock"]
RUN apt-get -y update; apt-get -y install curl
RUN curl -fsSL https://get.docker.com | sh
RUN curl -L "https://github.com/ubiquiti/docker-compose-aarch64/releases/download/1.22.0/docker-compose-Linux-aarch64" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose
COPY . /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/
WORKDIR /opt/prefect/ChildhoodCancerDataInitiative-Prefect_Pipeline/
