#!/usr/bin/env python
# coding: utf-8
import bonobo
import requests
import psycopg2
import redis
import time
import os
import socketio
import json
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '../.env')
load_dotenv(dotenv_path)

timescale_db_configuration = {
    "dbname": os.getenv("TIMESCALE_DB_NAME"),
    "user": os.getenv("TIMESCALE_USER"),
    "password": os.getenv("TIMESCALE_PASS"),
    "host": os.getenv("TIMESCALE_HOST"),
    "port": os.getenv("TIMESCALE_PORT")
}

tile38_db_configuration = {
    "dbname": os.getenv("TILE38_DB_NAME"),
    "user": os.getenv("TILE38_USER"),
    "password": os.getenv("TILE38_PASS"),
    "host": os.getenv("TILE38_HOST"),
    "port": os.getenv("TILE38_PORT")
}

client = redis.Redis(
    host=tile38_db_configuration["host"],
    port=tile38_db_configuration["port"],
    db=tile38_db_configuration["dbname"]
)

RADIUS = os.getenv("GEO_FENCING_RADIUS")

ROWS = os.getenv("OPEN_DATA_ROW_COUNT")

URL = os.getenv("OPEN_DATA_REAL_TIME_BUSES")

WS_HOST = os.getenv("WS_HOST")
WS_USER = os.getenv("WS_USER")

sio = socketio.Client()


def fetch_data():
    url = f"{URL}{ROWS}"
    for i in range(10):
        print(f"Iteration {i}/10")
        response = requests.get(url)
        for record in response.json().get("records"):
            result = {
                "vehicle_id": record["fields"]["idvh"],
                "lon": record["geometry"]["coordinates"][1],
                "lat": record["geometry"]["coordinates"][0],
                "type": record["fields"]["type"],
                "state": record["fields"]["etat"],
                "desserte_id": record["fields"]["iddesserte"],
                "stop_time": record["fields"]["harret"],
                "time": record["record_timestamp"]
            }
            yield result
        time.sleep(10)


def create_position(record: dict):
    """
    Saves a position in the database
    :param record:
    :return:
    """
    with psycopg2.connect(**timescale_db_configuration) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                INSERT INTO position (vehicle_id, lon, lat, type, state, desserte_id, stop_time, time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    record["vehicle_id"],
                    record["lon"],
                    record["lat"],
                    record["type"],
                    record["state"],
                    record["desserte_id"],
                    record["stop_time"],
                    record["time"]
                ))
            except psycopg2.IntegrityError:
                pass
    yield record


def connect_to_socket():
    sio.connect(WS_HOST)
    sio.emit('add user', "Donna Noble")
    yield None


def push_notification(record: dict):
    sio.emit('new message', {"username": WS_USER, "message": json.dumps(record)})


graph = bonobo.Graph()
# graph.add_chain(fetch_data, bonobo.PrettyPrinter())
graph.add_chain(connect_to_socket, fetch_data)
graph.add_chain(create_position, _input=fetch_data)
graph.add_chain(push_notification, _input=fetch_data)

r = bonobo.run(graph)
