#worker image
FROM node AS worker
RUN apt-get -qq update
RUN apt-get -qq -y install dnsutils
RUN npm install async
RUN npm install fast-csv
RUN npm install ioredis
RUN npm install --save @google-cloud/storage
RUN mkdir -p /app
COPY worker.js /app/
COPY package.json /app/
RUN mkdir /output
VOLUME /output
EXPOSE 8080
CMD ["node", "/app/worker.js"]