FROM jupyter/minimal-notebook:latest

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

USER ${NB_UID}
