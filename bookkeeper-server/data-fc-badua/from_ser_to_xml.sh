#!/bin/sh
BADUACLI="ba-dua-cli-0.6.0-all.jar"
BADUASER="../coverage.ser"
CLASSES="../target/classes"
BADUAXML="../target/badua.xml"

java -jar ${BADUACLI} report    \
        -input ${BADUASER}      \
        -classes ${CLASSES}     \
        -show-classes           \
        -show-methods           \
        -xml ${BADUAXML}        \
