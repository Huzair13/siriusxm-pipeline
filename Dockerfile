FROM amazon/aws-cli:latest

RUN yum update -y && \
    yum install -y yum-utils shadow-utils && \
    yum-config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo && \
    yum -y install terraform

RUN yum install -y python3 python3-pip

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["bash"]
