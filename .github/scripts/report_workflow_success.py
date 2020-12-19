#!/usr/bin/env python3
from utils.slack import SlackMessageSender
from utils import get_environment_variable, get_deployment_description, get_branch, get_project

if __name__ == '__main__':
    slack_sender = SlackMessageSender()
    run = get_environment_variable('GITHUB_RUN_ID')
    repository = get_environment_variable('GITHUB_REPOSITORY')
    sha = get_environment_variable('GITHUB_SHA')
    workflow_name = get_environment_variable('GITHUB_WORKFLOW')
    msg = f":tada: Github Actions *{repository}* workflow *{workflow_name}* run " \
          f"<https://github.com/{repository}/actions/runs/{run}|{run}> *succeeded* {get_deployment_description()}" \
          f"(commit `<https://github.com/{repository}/commit/{sha}|{sha[:8]}>`) " \
          f"Full reports can be found <https://sodadata.github.io/{get_project()}/{get_branch()}|here>."
    slack_sender.send_slack_message(msg)
