FROM node:latest
# Install node and java (for rml-runner)
RUN apt-get update
RUN apt-get install -y openjdk-11-jre-headless

RUN mkdir -p /app

WORKDIR /app

COPY package*.json ./
COPY tsconfig.json ./

RUN npm install

COPY src/ ./src
COPY bin/ ./bin
COPY processor/ ./processor

RUN npm run build

ARG file=run.ttl

COPY $file ./run.ttl

RUN node bin/docker.js ./run.ttl

ENTRYPOINT ["node","bin/js-runner.js","/tmp/run.ttl"]

