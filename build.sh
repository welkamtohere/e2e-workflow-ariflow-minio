#!/bin/bash
set -e

if [ ! -f adventureworks.zip ]; then
    echo "Downloading postgres AdventureWorks zip file from github.com/chriseaton/docker-adventureworks";
    wget "https://github.com/chriseaton/docker-adventureworks/releases/download/2025-06-19/chriseaton-docker-adventureworks-postgres.zip" -O adventureworks.zip -q;
    echo "Download complete."
else
    echo "postgres AdventureWorks file already downloaded. Skipping.";
fi
echo "Unzipping postgres AdventureWorks zip file.";
unzip -o adventureworks.zip;
rm adventureworks.zip;

chmod +x docker-init.sh 2>/dev/null || true
chmod +x *.sh 2>/dev/null || true

perl -pi -e 's/\r$//' docker-init.sh
perl -pi -e 's/\r$//' docker-reconfigure.sh

echo "Building postgres docker image.";
if [[ -f install-a.sql ]] && [[ -f install-a.sql ]] && [[ -f install-a.sql ]] && [[ -f install-a.sql ]]; then
    docker build  --progress=plain . -t chriseaton/adventureworks:postgres -t chriseaton/adventureworks:postgres-16;
else
    echo "Required SQL files not found. Please ensure install-a.sql, install-b.sql, install-c.sql, and install-d.sql are present.";
    exit 1;
fi
echo 0;
