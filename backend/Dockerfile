FROM python:3

ENV PYTHONUNBUFFERED

WORKDIR /code

COPY requirements.txt /code/

RUN pip install -r requirements.txt

COPY . /code/