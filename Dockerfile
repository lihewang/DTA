FROM launcher.gcr.io/google/nodejs
RUN npm install --save async
RUN npm install --save hashtable
RUN npm install --save fast-csv
RUN npm install --save redis
RUN npm install --save priorityqueuejs
RUN npm install --save express
RUN npm install --save body-parser
RUN npm install --save mathjs
RUN npm install --save redis-scripto
RUN npm install --save @google-cloud/storage
RUN npm install --save @google-cloud/logging
RUN npm install --save @google-cloud/debug-agent
RUN mkdir -p /app
WORKDIR /app
COPY package.json /app
COPY * /app/
EXPOSE 8080
CMD ["node", "/app/worker.js"]





