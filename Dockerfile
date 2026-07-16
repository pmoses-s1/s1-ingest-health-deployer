FROM python:3.11-slim
WORKDIR /srv
COPY . /srv
# The app binds 0.0.0.0 INSIDE the container (required for Docker port publishing to reach it).
# It is NOT authenticated by default, so publish only to the host loopback (see run command below).
ENV INGEST_PORT=8788 INGEST_HOST=0.0.0.0
EXPOSE 8788
# Distinct port from s1-ueba-deployer (8799) so both can run at once.
# Local use (recommended) - publish to the host loopback so only this machine can reach it:
#   docker run --rm -p 127.0.0.1:8888:8788 --env-file .env s1-ingest-health-deployer
# Network/shared use - require a token and opt in explicitly:
#   docker run --rm -p 8888:8788 -e INGEST_BIND_ALL=1 -e INGEST_AUTH_TOKEN=<secret> --env-file .env <img>
#   then open  http://<host>:8888/?token=<secret>
CMD ["python", "app/server.py"]
