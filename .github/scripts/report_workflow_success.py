#!/usr/bin/env python3

from utils.slack import SlackMessageSender

from utils import get_env, deployment_description

if __name__ == '__main__':
    slack_sender = SlackMessageSender()
    run = get_env('GITHUB_RUN_ID')
    repository = get_env('GITHUB_REPOSITORY')
    sha = get_env('GITHUB_SHA')
    workflow_name = get_env('GITHUB_WORKFLOW')
    msg = f":tada: Github Actions *{repository}* workflow *{workflow_name}* run " \
          f"<https://github.com/{repository}/actions/runs/{run}|{run}> *succeeded* {deployment_description()}" \
          f"(commit `<https://github.com/{repository}/commit/{sha}|{sha[:8]}>`)"
    slack_sender.send_slack_message(msg)
