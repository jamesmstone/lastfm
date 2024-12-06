FROM python:alpine
RUN pip install --force-reinstall -v "sqlite-utils==3.35.1" 
ENTRYPOINT ["sqlite-utils"]
