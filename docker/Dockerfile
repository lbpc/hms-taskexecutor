FROM ubuntu:14.04

RUN  apt-get update && \
     apt-get install -y --no-install-recommends curl bash tree python3-pip python-pip openjdk-7-jdk gcc \
                                                python-dev  build-essential libssl-dev libffi-dev \
                                                libxml2-dev libxslt1-dev zlib1g-dev python3-dev && \
     cd /bin && curl -L -O https://pantsbuild.github.io/setup/pants && chmod +x pants && \
     pip2 install --upgrade pip && pip3 install --upgrade pip && \
     pip3 install pylint && \
     mkdir /app && \
     /bin/pants -V && \
     apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /usr/share/doc/* /usr/share/man/* 
     



