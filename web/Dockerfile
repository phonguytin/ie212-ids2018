FROM python:3.12-slim
WORKDIR /opt/ids/web
COPY web/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY web/app /opt/ids/web/app
COPY src/ids /opt/ids/src/ids
ENV PYTHONPATH=/opt/ids/src:/opt/ids/web PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
