import logging
from random import randint
from time import sleep

logger = logging.getLogger(__name__)


def send_email(
        user_id: str,
        email: str,
        subject: str,
        message: str,
        external_id: str,
) -> None:
    """
    Simulates sending an email with a random delay between 5 and 20 seconds.
    """
    min_delay = 5
    max_delay = 20
    delay_seconds = randint(min_delay, max_delay)
    sleep(delay_seconds)
    print("Send EMAIL to=", email, "user_id=", user_id, "external_id=", external_id, "subject=", subject, "message_length=", len(message))

