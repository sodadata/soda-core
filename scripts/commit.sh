#!/usr/bin/env bash

if [ $# -eq 1 ]; then
    set -e
    set -x
    . .venv/bin/activate
    pre-commit run --all-files
    python -m pytest
    git status
    git add .
    git commit -m "$1"
    git pull
    git status
    echo
    read -p "Do you want to git push? y/N" -n 1 -r are_you_sure
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
