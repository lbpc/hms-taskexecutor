FROM docker-registry.intr/test-images/web:latest
RUN bash -c "virtualenv -p `which python3` te && \
				source te/bin/activate && \
				pip install --extra-index-url http://repo.intr:8181 taskexecutor"
CMD /usr/local/te/bin/taskexecutor

