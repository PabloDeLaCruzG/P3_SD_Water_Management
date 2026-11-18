FROM python:3.12-slim
WORKDIR /app
COPY WM_Central/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY WM_Central ./WM_Central
RUN mkdir -p /data
ENV DB_FILE=/data/wm_central.db PANEL_PORT=8080 PYTHONUNBUFFERED=1
EXPOSE 5050 8080
CMD ["sh", "-c", "python -m WM_Central.WM_Central 5050 ${KAFKA_BROKER}"]