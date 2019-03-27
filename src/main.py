#!/usr/bin/env python
# coding: utf-8
import bonobo
import requests
import psycopg2
import redis
import os
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

URL = os.getenv("OPEN_DATA_DESSERTES")


def timescale_setup():
    """
    Timescale DB Setup
    :return:
    """
    with psycopg2.connect(**timescale_db_configuration) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
            DROP TABLE IF EXISTS line CASCADE
            """)
            cursor.execute("""
            DROP TABLE IF EXISTS stop CASCADE
            """)
            cursor.execute("""
            DROP TABLE IF EXISTS desserte CASCADE
            """)
            cursor.execute("""
            DROP TABLE IF EXISTS position CASCADE
            """)

        with connection.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS line (
                id VARCHAR(256) PRIMARY KEY,
                name VARCHAR(256)
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stop (
                id VARCHAR(256) PRIMARY KEY,
                name VARCHAR(256),
                line_id VARCHAR(256) REFERENCES line (id),
                lon REAL,
                lat REAL
            );
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS desserte (
                id VARCHAR(256) PRIMARY KEY,
                line_id VARCHAR(256) REFERENCES line (id),
                stop_id VARCHAR(256) REFERENCES stop (id)
            );
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS position (
                time TIMESTAMP WITH TIME ZONE,
                vehicle_id VARCHAR(256),
                lon REAL,
                lat REAL,
                type VARCHAR(256),
                state VARCHAR(256),
                desserte_id VARCHAR(256) REFERENCES desserte (id),
                stop_time TIMESTAMP WITH TIME ZONE
            );
            """)

            cursor.execute("""SELECT create_hypertable('position', 'time');""")
    yield None


def tile38_setup():
    """
    Resets all the contents stored
    :return:
    """
    client.flushall
    yield None


def fetch_data():
    url = f"{URL}{ROWS}"
    response = requests.get(url)
    for record in response.json().get("records"):
        result = {
            "line_id": record["fields"]["mnemoligne"],
            "stop_id": record["fields"]["mnemoarret"],
            "line_name": record["fields"]["nomligne"],
            "stop_name": record["fields"]["nomarret"],
            "stop_lon": record["geometry"]["coordinates"][0],
            "stop_lat": record["geometry"]["coordinates"][1],
            "desserte_id": record["fields"]["iddesserte"]
        }
        yield result


def create_entities(record: dict):
    create_line(record)
    create_stop(record)
    create_desserte(record)
    yield record


def create_line(record: dict):
    with psycopg2.connect(**timescale_db_configuration) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                INSERT INTO line (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (record["line_id"], record["line_name"]))
            except psycopg2.IntegrityError:
                pass
    yield record


def create_stop(record: dict):
    with psycopg2.connect(**timescale_db_configuration) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                INSERT INTO stop (id, name, line_id, lon, lat) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    record["stop_id"],
                    record["stop_name"],
                    record["line_id"],
                    record["stop_lon"],
                    record["stop_lat"]
                ))
            except psycopg2.IntegrityError:
                pass
    yield record


def create_desserte(record: dict):
    with psycopg2.connect(**timescale_db_configuration) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                INSERT INTO desserte(id, line_id, stop_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """, (record["desserte_id"], record["line_id"], record["stop_id"]))
            except psycopg2.IntegrityError:
                pass
    yield record


def create_stop_tile38(_id: str, name: str, lat: float, long: float):
    result = client.execute_command(
        "SET",
        "stopList",
        _id,
        "NAME",
        name,
        "POINT",
        lat,
        long
    )
    yield result


def create_stop_fence(_id: str, lat: float, long: float):
    cmd = [
        "SETCHAN",
        "bus_stop",
        "NEARBY",
        "stopList",
        "POINT",
        lat,
        long,
        RADIUS
    ]
    result = client.execute_command(*cmd)
    yield result


def create_geo_fence_for_stop(record: dict):
    result = create_stop_fence(
        record["stop_id"],
        record["stop_lat"],
        record["stop_lon"]
    )
    yield result


def insert_stop(record):
    result = create_stop_tile38(
        record["stop_id"],
        record["stop_name"],
        record["stop_lat"],
        record["stop_lon"],
    )
    yield result


graph = bonobo.Graph()
graph.add_chain(
    timescale_setup,
    tile38_setup,
    fetch_data
)
graph.add_chain(create_entities, _input=fetch_data)
graph.add_chain(insert_stop, _input=fetch_data)
graph.add_chain(create_geo_fence_for_stop, _input=fetch_data)
r = bonobo.run(graph)
