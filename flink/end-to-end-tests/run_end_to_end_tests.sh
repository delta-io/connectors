#!/usr/bin/env bash

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

BASE_E2E_DIR=$(pwd)
TERRAFORM_DIR="terraform/"
TEST_DATA_LOCAL_DIR="test-data/"
TEST_DATA_S3_LOCATION="s3://databricks-performance-benchmarks-data/"

build_artifact() {
  cd ../../ || exit
  build/sbt flinkE2E/assembly
  local return_code=$?
  cd "$BASE_E2E_DIR" || exit
  return $return_code
}

create_terraform_infrastructure() {
  echo "Creating terraform infrastructure."
  terraform -chdir=$TERRAFORM_DIR init &&
  terraform -chdir=$TERRAFORM_DIR validate &&
  terraform -chdir=$TERRAFORM_DIR apply -auto-approve
}

destroy_terraform_infrastructure() {
  echo "Destroying terraform infrastructure."
  terraform -chdir=$TERRAFORM_DIR destroy -auto-approve
  echo "Terraform infrastructure destroyed."
}

copy_test_data_to_s3() {
  aws s3 cp $TEST_DATA_LOCAL_DIR $TEST_DATA_S3_LOCATION --recursive
}

run_end_to_end_tests() {
  echo "Running tests..."
  cd ../../
  build/sbt flinkE2E/test
}

main() {
  if ! create_terraform_infrastructure; then
    echo "[ERROR] Failed to create test infrastructure."
    exit 1
  fi

  if ! copy_test_data_to_s3; then
    echo "[ERROR] Failed to copy test data to S3."
    exit 1
  fi

  if ! build_artifact; then
    echo "[ERROR] Failed to build artifact."
    exit 1
  fi

  if ! run_end_to_end_tests; then
    echo "[ERROR] Failed to run tests."
    exit 1
  fi
}

cleanup() {
  echo "Clean up..."
  cd "$BASE_E2E_DIR" || exit 1
  # destroy_terraform_infrastructure
}

trap cleanup EXIT
main

