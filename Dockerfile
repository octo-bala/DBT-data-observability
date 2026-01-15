ARG BASE_IMAGE_TAG=0.2.2
FROM streemdatagridcommonacr.azurecr.io/python-base:${BASE_IMAGE_TAG} AS base

# Add the git commit hash
ARG GIT_COMMIT=unspecified
ENV GIT_COMMIT=$GIT_COMMIT

# Add the docker image tag
ARG DOCKER_IMAGE_TAG=unspecified
ENV DOCKER_IMAGE_TAG=$DOCKER_IMAGE_TAG


RUN pip install --no-cache-dir \
    dagster==1.11.5 \
    dagster-graphql==1.11.5 \
    dagster-webserver==1.11.5 \
    dagster_postgres==0.27.5

# Set $DAGSTER_HOME and copy dagster.yaml and workspace.yaml there
ENV DAGSTER_HOME=/opt/dagster/.dagster/

RUN mkdir -p $DAGSTER_HOME

COPY --from=config dagster_docker.yaml $DAGSTER_HOME/dagster.yaml
COPY --from=config workspace_docker.yaml $DAGSTER_HOME/workspace.yaml

WORKDIR $DAGSTER_HOME
