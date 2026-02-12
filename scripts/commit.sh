#!/usr/bin/env bash

if [ $# -eq 1 ]; then
    uv run --group dev pre-commit run --all-files
    PRE_COMMIT_RESULT=$?
    set -e
    if [ $PRE_COMMIT_RESULT -ne 0 ]; then
        echo pre-commit failed.  Trying once more.
        uv run --group dev pre-commit run --all-files
    else
        echo First time ok
    fi
    uv run --group dev pytest soda-tests/
    git add .
    git status
    git commit -m "$1"
    git pull
    git status
    echo
    read -p "Do you want to git push? (y/N): " -n 1 -r are_you_sure
    echo    # (optional) move to a new line
    if [[ $are_you_sure =~ ^[Yy]$ ]]
    then
        git push
    else
        echo "Skipping git push"
    fi
else
    echo "Error: 1 argument required: the commit message"
    exit 1
fi
