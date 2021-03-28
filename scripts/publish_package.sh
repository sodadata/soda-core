#!/usr/bin/env bash
# A hacky script to publish all packages to testpypi
pushd .
cd core
echo "Publishing core"
rm -fr dist
python setup.py sdist
twine upload --repository testpypi dist/*
popd

cd packages
for pack in *
do
  echo "Publishing ${pack}"
  pushd .
  cd $pack
  rm -fr dist
  python setup.py sdist
  twine upload --repository testpypi dist/*
  popd
done
