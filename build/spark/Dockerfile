FROM apache/spark:4.0.5-python3
USER root
COPY deps/jars/ /opt/spark/jars/
COPY deps/python/delta/ /opt/spark/python/delta/
ENV PYTHONPATH=/opt/spark/python
USER 185
