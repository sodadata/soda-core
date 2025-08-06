#!/bin/bash

printf "["; find . -maxdepth 1 -mindepth 1 -type d -name "soda*" ! -name "soda-databricks" -printf '"%f",' | sed 's/,$//'; printf "]"
