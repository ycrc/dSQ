#!/bin/bash
shopt -s expand_aliases
source /etc/yalehpc
DSQ_CV=1.03
DSQ_NV=$( echo "${DSQ_CV} + 0.01" | bc )


module load EasyBuild && \
      eb dSQ-${DSQ_CV}.eb --try-software-version=${DSQ_NV} && \
      cd ~/repos/ycrc-ebfiles && \
      git pull  && \
      git add easyconfigs/dSQ/dSQ-${DSQ_NV}.eb && \
      git commit -m "installed dSQ v${DSQ_NV} on ${cluster}" && \
      git push

