# coding: utf-8
import signal
import unittest
import os
import time
from multiprocessing import Process
from typing import Union, NewType

import docker
from datetime import datetime

Seconds = NewType("Seconds", int)


def kill_service_after(p: Process, timeout: Seconds):
    os.kill(p.pid, signal.SIGTERM)

    time.sleep(timeout)
    if p.is_alive():
        os.kill(p.pid, signal.SIGKILL)

    p.join(timeout=1)


def get_docker_client() -> docker.APIClient:
    return docker.APIClient()


def container_healthy(client: docker.APIClient, container_id: str) -> Union[Exception, bool]:
    return client.inspect_container(container_id).get("State", {}).get("Health", {}).get("Status") == "healthy"


class EmbeddedPostgresTestcase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        docker_client = get_docker_client()

        container_start_options = {
            'detach': True,
            'network_mode': 'host',
            'remove': True,
            'environment': {"POSTGRES_PASSWORD": "secret"}
        }

        containers = docker.from_env().containers

        cls.postgres_container = containers.run(
            image='postgres:10',
            **container_start_options
        )
        # TODO: requires source container to support health checks
        # while not container_healthy(docker_client, cls.postgres_container.id):
        #     pass
        print(f"{datetime.now()} waiting for docker container {cls.postgres_container.name} to start")
        time.sleep(15)
        print(f"{datetime.now()} considering container started")

        docker_client.close()

    @classmethod
    def tearDownClass(cls):
        cls.postgres_container.stop()
