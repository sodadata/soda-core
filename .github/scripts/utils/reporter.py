#!/usr/bin/env python3

import fnmatch
import os
import xml.etree.ElementTree as elementTree

import requests

from . import ENV
from . import SlackMessageSender


class Reporter:
    endpoint: str
    job: str
    run: str
    root_dir: str
    sender: SlackMessageSender

    def __init__(self):
        self.sender = SlackMessageSender()
        self.job = ENV.get_variable('JOB_ID')
        self.run = ENV.get_variable('GITHUB_RUN_ID')
        self.repository = ENV.get_variable('GITHUB_REPOSITORY')
        self.sha = ENV.get_variable('GITHUB_SHA')
        self.token = ENV.get_variable('GITHUB_TOKEN')
        self.workflow_name = ENV.get_variable('GITHUB_WORKFLOW')
        self.root_dir = os.path.join(os.path.dirname(__file__), '../')

    def send_slack_message(self, msg: str):
        self.sender.send_slack_message(msg)

    def report_workflow_failure(self):
        author = self._find_author()
        msg = self._status_message(":cry:") \
            + self._run_message() \
            + f" *failed* {ENV.get_deployment_description()} on job `{self.job}` " \
            + self._commit_message() \
            + f"Last author was {author}. " \
            + self._reports_message()
        self.send_slack_message(msg)
        for r in self._find_files('TEST*.xml'):
            self._process_xml(r)

    def report_workflow_success(self):
        msg = self._status_message(":tada:") \
            + self._run_message() \
            + f"*succeeded* {ENV.get_deployment_description()}" \
            + self._commit_message() \
            + self._reports_message()
        self.send_slack_message(msg)

    def _run_message(self):
        return f"<https://github.com/{self.repository}/actions/runs/{self.run}|{self.run}> "

    def _status_message(self, status):
        return f"{status} Github Actions *{self.repository}* workflow *{self.workflow_name}* run "

    def _commit_message(self):
        return f"(commit `<https://github.com/{self.repository}/commit/{self.sha}|{self.sha[:8]}>`) "

    @staticmethod
    def _reports_message():
        return f"Full reports can be found <{ENV.get_reports_url()}/{ENV.get_branch()}|here>."

    def _find_files(self, pattern: str):
        root = self.root_dir
        for root, dirs, files in os.walk(root):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    yield os.path.join(root, name)

    def _process_xml(self, xml_report: str):
        tree = elementTree.parse(xml_report)
        for error_test_case in tree.findall('.//testcase[error]'):
            self._process_failure(error_test_case, failure_type='error')
        for failed_test_case in tree.findall('.//testcase[failure]'):
            self._process_failure(failed_test_case, failure_type='failure')

    def _process_failure(self, test_case, failure_type):
        fail_class = test_case.attrib['classname']
        fail_method = test_case.attrib['name']
        stack = test_case.find(failure_type).text.strip()
        self._send_slack_message_error(fail_class, fail_method, stack.strip(), failure_type)

    def _send_slack_message_error(self, fail_class, fail_method, stack, failure_type):
        msg = f"Test `{fail_class}#{fail_method}` experienced {failure_type} with:\n```\n{stack}\n```."
        self.send_slack_message(msg)

    def _find_author(self):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }
        r = requests.get(f'https://api.github.com/repos/{self.repository}/actions/runs/{self.run}',
                         headers=headers)
        email = r.json()['head_commit']['author']['email']
        return f'`{email}`'
