# QuestDB server built from source at the commit that first ships LIVE VIEW.
# (Not yet in questdb/questdb:nightly at the time of writing. Once this merges to
# main and a nightly picks it up, replace this whole build with a one-liner:
#     image: questdb/questdb:nightly
# in docker-compose.yml and delete this file.)
#
# Pinned by SHA so the image is reproducible. This commit is expected to merge to
# main, which keeps the SHA reachable (and therefore fetchable) permanently.
ARG QDB_COMMIT=90a1b54c98b10fad5304b1ad817a69cda25e52ad

FROM eclipse-temurin:25-jdk AS build
ARG QDB_COMMIT
RUN apt-get update && apt-get install -y --no-install-recommends \
        git maven cmake nasm build-essential ca-certificates \
        nodejs npm \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java/openjdk

WORKDIR /src
RUN git clone https://github.com/questdb/questdb.git questdb
WORKDIR /src/questdb
RUN git checkout "${QDB_COMMIT}"
RUN git submodule update --init --recursive

# Build the bundled java client native lib first (matches the documented build:
# cmake in java-questdb-client/core, then mvn install that module), then package
# the server with the web console (-P build-web-console needs Node/npm, installed
# above), so http://<host>/ serves the QuestDB console.
WORKDIR /src/questdb/java-questdb-client/core
RUN cmake -DCMAKE_BUILD_TYPE=Release -B cmake-build-release -S . \
    && cmake --build cmake-build-release --config Release
WORKDIR /src/questdb/java-questdb-client
RUN mvn -q -B install -DskipTests
WORKDIR /src/questdb
RUN mvn -B package -DskipTests -P build-web-console
# The runnable server jar (exclude sources/tests/javadoc classifiers).
RUN cp "$(ls core/target/questdb-*-SNAPSHOT.jar | grep -Ev 'sources|tests|javadoc' | head -1)" /questdb.jar

FROM eclipse-temurin:25-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m qdb \
    && mkdir -p /var/lib/questdb && chown qdb /var/lib/questdb
COPY --from=build /questdb.jar /questdb.jar
USER qdb
EXPOSE 9000
# QuestDB requires Java 25 (core/pom.xml: javac.target=25, and the enforcer's
# java25+ profile activates on jdk (24,)). This exact flag set is the project's
# documented start command.
ENTRYPOINT ["java", \
    "--sun-misc-unsafe-memory-access=allow", \
    "--enable-native-access=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=java.base/java.time.zone=ALL-UNNAMED", \
    "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", \
    "-jar", "/questdb.jar", "-d", "/var/lib/questdb"]
