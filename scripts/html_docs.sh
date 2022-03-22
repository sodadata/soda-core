#!/usr/bin/env bash

source .venv/bin/activate

pip install gh-md-to-html

generate_html () {
  gh-md-to-html "docs/$1.md" -d docs/tmp/html -c
  cat docs/build_docs_styles.txt "docs/tmp/html/$1.html" > "docs/html/$1.html"
  open "docs/html/$1.html"
}

generate_html "running_a_scan"
generate_html "soda_checks_yaml"
generate_html "soda_environment_yaml"
