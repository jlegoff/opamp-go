FROM ubuntu:jammy

WORKDIR /src
COPY . /src

RUN apt-get -y update
RUN apt-get -y install jq golang make ca-certificates
RUN make build-example-supervisor
RUN mkdir /nrdot
RUN cp /src/internal/examples/supervisor/bin/supervisor /nrdot
RUN cp /src/entrypoint.sh /nrdot
RUN chmod +x /nrdot/entrypoint.sh

WORKDIR /nrdot
ENTRYPOINT exec ./entrypoint.sh
