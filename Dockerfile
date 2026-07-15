FROM python:3.11-slim
WORKDIR /srv
COPY . /srv
ENV INGEST_PORT=8788 INGEST_HOST=0.0.0.0
EXPOSE 8788
# Distinct port from s1-ueba-deployer (8799) so both can run at once.
# Pass credentials at runtime, e.g.:
#   docker run --rm -p 8888:8788 --env-file .env s1-ingest-health-deployer
CMD ["python", "app/server.py"]
