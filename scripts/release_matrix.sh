#!/bin/bash

printf "["; find . -maxdepth 1 -mindepth 1 -type d -name "soda*" -not -name "soda-trino" -printf '"%f",' | sed 's/,$//'; printf "]"
