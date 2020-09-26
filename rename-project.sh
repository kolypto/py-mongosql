#! /usr/bin/env bash
set -e

if [[ $# -eq 0 ]] ; then
    echo "Usage: $0 new-project-name"
    exit 255
fi

PROJECT_NAME=$1

# Rename
mv myproject $PROJECT_NAME
fgrep -rnl 'myproject' * | xargs sed -i "s/myproject/$PROJECT_NAME/g"

# Remove myself
rm $0
