#!/usr/bin/env python3

import fnmatch
import os
import xml.etree.ElementTree as elementTree
import requests

from utils.slack import SlackMessageSender
from utils import get_env, deployment_description


class Reporter:
    endpoint: str
    job: str
    run: str
    root_dir: str
    sender: SlackMessageSender

    def __init__(self):
        self.sender = SlackMessageSender()
        self.job = get_env('JOB_ID')
        self.run = get_env('GITHUB_RUN_ID')
        self.repository = get_env('GITHUB_REPOSITORY')
        self.sha = get_env('GITHUB_SHA')
        self.token = get_env('GITHUB_TOKEN')
        self.workflow_name = get_env('GITHUB_WORKFLOW')
        self.root_dir = os.path.join(os.path.dirname(__file__), '../')
        self.test_reports_url = 'https://sodadata.github.io/sodasql/reports/tests'
        self.branch = os.path.basename(get_env('GITHUB_REF'))

    def send_slack_message(self, msg: str):
        self.sender.send_slack_message(msg)

    def report_workflow_failure(self):
        author = self._find_author()
        msg = f":cry: Github Actions *{self.repository}* workflow *{self.workflow_name}* run " \
              f"<https://github.com/{self.repository}/actions/runs/{self.run}|{self.run}>" \
              f" *failed* {deployment_description()}on job `{self.job}` " \
              f"(commit `<https://github.com/{self.repository}/commit/{self.sha}|{self.sha[:7]}>`). " \
              f"Last author was {author}." \
              f"Full test reports can be found <{self.test_reports_url}/{self.branch}/index.html|here>."
        self.send_slack_message(msg)
        for r in self._find_files('TEST*.xml'):
            self._process_xml(r)

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


if __name__ == '__main__':
    reporter = Reporter()
    reporter.report_workflow_failure()
