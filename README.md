# mqkit

## Introduction
`mqkit` is a Python framework for creating apps that integrate with message brokers
like RabbitMQ. It provides a FastAPI-style interface to accelerate the development
of queue-based services.

## Documentation
Complete documentation will be coming soon.

## Usage
### Multi-thread, single process example
```python
from typing import Dict

from mqkit import App, Attributes, create_engine

app: App = App()


@app.on_start
def on_start() -> None:
    print("App is starting")


@app.queue("my_queue", forward_to="other_queue")
def my_queue_handler(message: Dict, attributes: Attributes) -> Dict:
    print(f"Received {message} with attributes {attributes}")
    return {"hello": "other queue!"}


@app.queue("other_queue")
def other_queue_handler(message: Dict, attributes: Attributes) -> None:
    print(f"Other queue received {message}")


app.run(create_engine("amqp://user:password@your-server:5672/"))
```

### Single-thread, single process example
The blocking single-threaded `@consume` decorator is intended for situations where
orchestration is handled by an external provider. (Think Kubernetes or Docker)

```python

from typing import Dict

from mqkit import Attributes, consume


@consume("my_queue")
def handler(message: Dict, attributes: Attributes) -> None:
    print(f"Got message {message}")


# NOTE: The engine URL can be inferred here based on the MQKIT_ENGINE_URL
# environment variable. If you don't want to use the environment variable,
# pass an Engine instance as the `engine` parameter to @consume

```

### Parameter models
`mqkit` also supports automatic serialization and validation of queue messages using
Pydantic `BaseModel` classes. The appropriate models to use are inferred based on
the annotations of the handler method:

```python

from datetime import datetime

from mqkit import Attributes, consume
from pydantic import BaseModel


class ChatMessage(BaseModel):
    id: int
    user: str
    content: str
    sent_time: datetime


@consume("chat_messages")
def handler(message: ChatMessage, attributes: Attributes) -> None:
    print(f"Got chat message {message!r} with attributes {attributes}")


"""
Invalid message raise exceptions and don't call the handler:

'invalid message' -> DecodeError
{}                -> ValidationError
{"id": 123}       -> ValidationError

Valid messages will result in the handler being called:
{
    "id": 123,
    "user": "will",
    "content": "hello!",
    "sent_time": "2026-03-09T11:51:09"
} -> Outputs the message with print()
"""
```

Note that a `BaseModel` annotation may also be added to the return
parameter which will enforce that return values are of that type.
