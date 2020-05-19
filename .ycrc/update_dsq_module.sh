#!/bin/bash
#set -o xtrace
source /etc/yalehpc
DSQ_CV=$( module load dSQ && dsq --version 2>&1 | cut -d" " -f2 )
DSQ_NV=$( curl --silent "https://api.github.com/repos/ycrc/dSQ/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/' )

if [ ! $DSQ_CV == $DSQ_NV ]
then
    cd ~/repos/ycrc-ebfiles && \
      git pull  && \
      module load EasyBuild && \
      eb dSQ-${DSQ_CV}.eb --try-software-version=${DSQ_NV} && \
      git add easyconfigs/dSQ/dSQ-${DSQ_NV}.eb && \
      git commit -m "installed dSQ v${DSQ_NV} on ${cluster}" && \
      git push
else
    echo "dSQ is up to date!"
fi
