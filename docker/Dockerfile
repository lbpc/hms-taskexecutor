FROM python:alpine

RUN  apk --no-cache add curl bash tree && cd /bin && \
     curl -L -O https://pantsbuild.github.io/setup/pants && \
     chmod +x pants && \
     pip3 install pylint && \
     mkdir /app



