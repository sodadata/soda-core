import json
import logging
import os
import ssl

import requests

from utils.utils import get_env


class SlackMessageSender:
    slack_webhook_url: str
    branch_or_tag: str
    ctx: ssl.SSLContext

    def __init__(self):
        self.slack_webhook_url = get_env('SLACK_WEBHOOK_URL')
        self.force_send = os.environ.get('FORCE_SEND', 'false')
        self.ctx = SlackMessageSender._create_non_verifying_context()
        self.branch_or_tag = get_env('GITHUB_REF')

    def send_slack_message(self, msg: str):
        payload = {"text": msg}
        if self.branch_or_tag == "refs/heads/master" \
                or self.branch_or_tag == "refs/heads/main" \
                or self.branch_or_tag.startswith("refs/tags/") \
                or self.force_send == "true":
            response = requests.post(self.slack_webhook_url, data=json.dumps(payload),
                                     headers={'Content-Type': 'application/json'})
            if response.status_code != 200:
                logging.error(f'Request to slack returned an error {response.status_code}, '
                              f'the response is:\n{response.text}')
        else:
            for e, v in os.environ.items():
                print(f"{e}={v}")
            print(f"Ignoring message '{msg}' since on branch {self.branch_or_tag}")

    @staticmethod
    def _create_non_verifying_context() -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
