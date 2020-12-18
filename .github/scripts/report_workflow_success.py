#!/usr/bin/env python3
import os
import urllib.parse

from utils.slack import SlackMessageSender
from utils import get_environment_variable, get_deployment_description

if __name__ == '__main__':
    slack_sender = SlackMessageSender()
    run = get_environment_variable('GITHUB_RUN_ID')
    repository = get_environment_variable('GITHUB_REPOSITORY')
    sha = get_environment_variable('GITHUB_SHA')
    workflow_name = get_environment_variable('GITHUB_WORKFLOW')
    test_reports_base_url = 'https://sodadata.github.io/sodasql/tests'
    branch = os.path.basename(get_environment_variable('GITHUB_REF'))
    test_reports_url = f'{test_reports_base_url}/{urllib.parse.quote_plus(branch)}/'
    msg = f":tada: Github Actions *{repository}* workflow *{workflow_name}* run " \
          f"<https://github.com/{repository}/actions/runs/{run}|{run}> *succeeded* {get_deployment_description()}" \
          f"(commit `<https://github.com/{repository}/commit/{sha}|{sha[:8]}>`)" \
          f"Full test reports can be found <{test_reports_url}|here>."
    slack_sender.send_slack_message(msg)
