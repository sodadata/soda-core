import json
import logging
import os
import ssl
import requests

from .env import ENV


class SlackMessageSender:
    slack_webhook_url: str
    branch: str
    ctx: ssl.SSLContext
    branches_to_notify = ['master', 'main']

    def __init__(self):
        self.slack_webhook_url = ENV.get_variable('SLACK_WEBHOOK_URL')
        self.force_send = os.environ.get('FORCE_SEND', 'false')
        self.ctx = SlackMessageSender._create_non_verifying_context()
        self.branch = ENV.get_branch()

    def send_slack_message(self, msg: str):
        payload = {"text": msg}
        if (self.branch in self.branches_to_notify) or self.force_send == "true":
            response = requests.post(self.slack_webhook_url, data=json.dumps(payload),
                                     headers={'Content-Type': 'application/json'})
            if response.status_code != 200:
                logging.error(f"Request to slack returned an error '{response.status_code}', "
                              f"the response is:\n'{response.text}'")
        else:
            logging.warning("Ignoring message '%s' since on branch '%s'", msg, self.branch)

    @staticmethod
    def _create_non_verifying_context() -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
