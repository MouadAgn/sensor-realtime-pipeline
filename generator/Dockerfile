FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY topology.py generator.py build_referentiel.py ./

# -u : logs non bufferisés (visibles en direct dans `docker compose logs`)
ENTRYPOINT ["python", "-u", "generator.py"]
