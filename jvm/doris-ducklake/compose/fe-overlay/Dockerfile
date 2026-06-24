# FE overlay image for the DuckLake-on-Doris live smoke.
#
# Overlays our locally-built P-series FE (`output/fe`, from branch-catalog-spi
# with the two ducklake FE patches applied — see ../fe-patches/FE-PATCHES.md)
# onto the stock released FE image, so the compose cluster runs our connector-SPI
# FE while inheriting the base image's init scripts / JDK / entrypoint.
#
# Build (stage a minimal context so the multi-GB repo isn't sent to the daemon):
#   S=/tmp/feimg; rm -rf $S; mkdir -p $S/output
#   cp -r ~/DEV/OSS/doris-catalog-spi/output/fe $S/output/fe
#   docker build --platform linux/amd64 \
#     -f jvm/doris-ducklake/compose/fe-overlay/Dockerfile \
#     -t doris-fe:pr62767-local \
#     --build-arg BASE_IMAGE=apache/doris:fe-4.1.0 --build-arg OUTPUT_PATH=./output $S
#
# Rebuild only when the FE itself changes; plugin-only changes are reinstalled
# into the plugin volume by smoke.sh.

ARG BASE_IMAGE=apache/doris:fe-4.1.0
FROM ${BASE_IMAGE}

ARG OUTPUT_PATH=./output

# DORIS_HOME/fe in the base image. Wipe the base build artifacts we replace,
# then overlay ours. init_fe.sh (in /opt/apache-doris) + the JDK come from the base.
ENV DORIS_FE_HOME=/opt/apache-doris/fe

RUN rm -rf ${DORIS_FE_HOME}/bin ${DORIS_FE_HOME}/lib ${DORIS_FE_HOME}/conf \
           ${DORIS_FE_HOME}/plugins ${DORIS_FE_HOME}/webroot

COPY ${OUTPUT_PATH}/fe/bin     ${DORIS_FE_HOME}/bin
COPY ${OUTPUT_PATH}/fe/lib     ${DORIS_FE_HOME}/lib
COPY ${OUTPUT_PATH}/fe/conf    ${DORIS_FE_HOME}/conf
COPY ${OUTPUT_PATH}/fe/plugins ${DORIS_FE_HOME}/plugins
COPY ${OUTPUT_PATH}/fe/webroot ${DORIS_FE_HOME}/webroot
