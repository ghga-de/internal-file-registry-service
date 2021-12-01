# Copyright 2021 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pub sub fixtures"""

import copy
import signal
from datetime import datetime
from typing import Callable, Generator, Optional

import pytest
from ghga_service_chassis_lib.pubsub import AmqpTopic, PubSubConfigBase
from ghga_service_chassis_lib.pubsub_testing import RabbitMqContainer

from .config import DEFAULT_CONFIG
from .storage import EXISTING_OBJECTS


def create_fake_drs_uri(object_id: str):
    """Create a fake DRS URI based on an object id."""
    return f"drs://www.example.org/{object_id}"


TEST_MESSAGES = {
    "non_staged_file_requested": [
        {
            "drs_id": create_fake_drs_uri(EXISTING_OBJECTS[0].object_id),
            "file_id": EXISTING_OBJECTS[0].object_id,
            "grouping_label": EXISTING_OBJECTS[0].object_id,
            "request_id": "my_test_stage_request_001",
            "timestamp": datetime.now().isoformat(),
        }
    ]
}


def raise_timeout_error(_, __):
    """Raise a TimeoutError"""
    raise TimeoutError()


def exec_with_timeout(
    func: Callable,
    timeout_after: int,
    func_args: Optional[list] = None,
    func_kwargs: Optional[dict] = None,
):
    """
    Exec a function (`func`) with a specified timeout (`timeout_after` in seconds).
    If the function doesn't finish before the timeout, a TimeoutError is thrown.
    """

    if func_args is None:
        func_args = {}
    if func_kwargs is None:
        func_kwargs = {}

    # set a timer that raises an exception if timed out
    signal.signal(signal.SIGALRM, raise_timeout_error)
    signal.alarm(timeout_after)

    # execute the function
    result = func(*func_args, **func_kwargs)

    # disable the timer
    signal.alarm(0)

    return result


class MessageSuccessfullyReceived(RuntimeError):
    """This Exception can be used to signal that the message
    was successfully received.
    """

    ...


def message_processing_wrapper(
    func: Callable,
    received_message: dict,
    expected_message: dict,
):
    """
    This function is used in the AmqpFixture to wrap the function `func` specified for
    processing incomming message.
    """
    assert (
        received_message == expected_message
    ), "The published message did not match the received message."

    func(received_message)
    raise MessageSuccessfullyReceived()


class TestPubSubClient:
    """A base class used to simulate publishing or subscribing services."""

    def __init__(
        self,
        config: PubSubConfigBase,
        subscriber_service_name: str,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """
        This does not only create a AmqpTopic object that is later used for
        publishing/subscribing but it also already initializes the channel that will be
        used for subscription.
        """

        self.config = config
        self.topic_name = topic_name
        self.message_schema = message_schema
        self.subscriber_service_name = subscriber_service_name

        # create topic later used for publishing/subscribing:
        self.topic = AmqpTopic(
            config=self.config,
            topic_name=self.topic_name,
            json_schema=self.message_schema,
        )

        # initialize the channel that is later used for subscription:
        subscriber_topic: AmqpTopic
        if self.config.service_name == subscriber_service_name:
            subscriber_topic = self.topic
        else:
            subscriber_config = copy.deepcopy(self.config)
            subscriber_config.service_name = self.subscriber_service_name
            subscriber_topic = AmqpTopic(
                config=subscriber_config,
                topic_name=self.topic_name,
            )

        subscriber_topic.init_subscriber_queue()


class TestPublisher(TestPubSubClient):
    """A class simulating a service that publishes to the specified topic."""

    def __init__(
        self,
        config: PubSubConfigBase,
        subscriber_service_name: str,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """Initialize the test publisher."""
        super().__init__(
            config=config,
            message_schema=message_schema,
            topic_name=topic_name,
            subscriber_service_name=subscriber_service_name,
        )

    def publish(self, message: dict):
        """publish a message"""

        self.topic.publish(message)


class TestSubscriber(TestPubSubClient):
    """A class simulating a service that subscribes to the specified topic."""

    def __init__(
        self,
        config: PubSubConfigBase,
        topic_name: str,
        message_schema: Optional[dict] = None,
    ):
        """Initialize the test subscriber."""
        super().__init__(
            config=config,
            message_schema=message_schema,
            topic_name=topic_name,
            subscriber_service_name=config.service_name,
        )

    def subscribe(
        self,
        expected_message: Optional[dict] = None,
        timeout_after: int = 2,
    ) -> dict:
        """
        Subscribe to the channel and expect the specified message (`exected_message`).
        A TimeoutError is thrown after the specified number of seconds (`timeout_after`).
        It returns the received message.
        """

        message_to_return = {}  # will be filled by the `process_message` function

        def process_message(message: dict, update_with_message: dict):
            """Process the incoming message and update the `update_with_message``
            with the message content"""
            if expected_message is not None:
                assert (
                    message == expected_message
                ), "The content of the received message didn't match the expectation."

            update_with_message.update(message)

        exec_with_timeout(
            func=self.topic.subscribe,
            func_kwargs={
                "exec_on_message": lambda message: process_message(
                    message, message_to_return
                ),
                "run_forever": False,
            },
            timeout_after=timeout_after,
        )

        # return the `message_to_return` dict that was populated by the
        # `process_message` function:
        return message_to_return


class AmqpFixture:
    """Info yielded by the `amqp_fixture` function"""

    def __init__(self, config: PubSubConfigBase) -> None:
        """Initialize fixture"""
        self.config = config

    def get_test_publisher(
        self,
        topic_name: str,
        service_name: str = "upstream_publisher",
        message_schema: Optional[dict] = None,
    ):
        """
        Get a TestPublisher object that simulates a service that publishes to the
        specified topic.
        Please note, the function has to be called before calling the subscribing
        function.
        """

        pub_config = PubSubConfigBase(
            rabbitmq_host=self.config.rabbitmq_host,
            rabbitmq_port=self.config.rabbitmq_port,
            service_name=service_name,
        )

        return TestPublisher(
            config=pub_config,
            subscriber_service_name=self.config.service_name,
            topic_name=topic_name,
            message_schema=message_schema,
        )

    def get_test_subscriber(
        self,
        topic_name: str,
        service_name: str = "downstream_subscriber",
        message_schema: Optional[dict] = None,
    ):
        """
        Get TestSubscriber object that simulates a service that subscribes to the
        specified topic.
        Please note, the function has to be called before publishing a message to the
        specified topic.
        """
        sub_config = PubSubConfigBase(
            rabbitmq_host=self.config.rabbitmq_host,
            rabbitmq_port=self.config.rabbitmq_port,
            service_name=service_name,
        )

        return TestSubscriber(
            config=sub_config,
            topic_name=topic_name,
            message_schema=message_schema,
        )


def amqp_fixture_factory(service_name: str = "my_service"):
    """A factory for creating Pytest fixture for working with AMQP."""

    @pytest.fixture
    def amqp_fixture() -> Generator[AmqpFixture, None, None]:
        """Pytest fixture for working with AMQP."""

        with RabbitMqContainer() as rabbitmq:
            connection_params = rabbitmq.get_connection_params()

            config = PubSubConfigBase(
                rabbitmq_host=connection_params.host,
                rabbitmq_port=connection_params.port,
                service_name=service_name,
            )

            yield AmqpFixture(config=config)

    return amqp_fixture


amqp_fixture = amqp_fixture_factory(service_name=DEFAULT_CONFIG.service_name)
