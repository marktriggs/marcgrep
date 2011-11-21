#!/bin/bash

lein_url=https://raw.github.com/technomancy/leiningen/stable/bin/lein

function get_lein
{
    if [ "`which wget`" != "" ]; then
        wget -c "$lein_url"
    elif [ "`which curl`" != "" ]; then
        curl "$lein_url" > lein
    else
        echo "ERROR: Couldn't find wget or curl on your path.  Aborting, sorry."
        exit
    fi
}

set -e

cd "`dirname $0`"

mkdir -p scripts
(cd scripts && get_lein && chmod a+x lein)

export LEIN_HOME="$PWD/.lein"
export JAVA_OPTS="-Dmaven.repo.local=$PWD/.m2"
scripts/lein uberjar
