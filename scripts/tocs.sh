#!/usr/bin/env bash

generate_toc () {
  echo ""
  echo $1
  HEADERS=$(egrep -i '^##+ ' $1)
  LEFT_PARTS=$(echo "$HEADERS" | sed -e 's/# /* /g' -e 's/#/  /g' -e 's/* \(.*\)/* [\1]/g')
  RIGHT_PARTS=$(echo "$HEADERS" | sed -E 's/#+ //g' | sed -E 's/([a-zA-Z]) /\1-/g' | sed -E 's/(.*)/(#\1)/g')
  paste <(echo "$LEFT_PARTS") <(echo "$RIGHT_PARTS") | sed 's/\t//'
}

generate_toc "docs/soda_checks_yaml.md"
generate_toc "docs/soda_configuration_yaml.md"
generate_toc "docs/running_a_scan.md"
generate_toc "docs/reference.md"
