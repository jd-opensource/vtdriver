#!/bin/bash

# Copyright 2021 JD Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pull vitess source to local and build a vtdriver-env docker image.

set -e

git --version >/dev/null 2>&1 || fail "git is not installed"
docker --version >/dev/null 2>&1 || fail "docker is not installed"

# which vitess release tag to use
release='v12.0.2'

echo "using branch/tag '${release}'"

if [ ! -d "build_vitess" ];then
  mkdir build_vitess
else
  rm -rf build_vitess
  mkdir build_vitess
fi

chmod -R ug+rwx ../vitess_env

cd build_vitess
echo "Downloading vitess source from github..."
git clone git@github.com:vitessio/vitess.git
cd vitess
git checkout ${release}

cp -r ../../vtdriver ./examples/local/
cp -r ../../Dockerfile.vtdriver ./docker/local/

# add target 'docker_vtdriver' in Makefile
echo -e '\ndocker_vtdriver:\n\t${call build_docker_image,docker/local/Dockerfile.vtdriver,vitess/vtdriver-env}\n' >> ./Makefile

echo "build docker image 'vitess/vtdriver-env'"
make docker_vtdriver

echo "Done."
