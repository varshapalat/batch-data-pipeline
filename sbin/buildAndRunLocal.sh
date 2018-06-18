#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building spark JAR===="
 cd  $DIR/../tw-pipeline && sbt package
echo "====Running docker-compose===="
cd $DIR/../docker && docker-compose up -d
