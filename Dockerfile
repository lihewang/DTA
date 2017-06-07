FROM launcher.gcr.io/google/nodejs
RUN npm install async
RUN npm install hashtable
RUN npm install fast-csv
RUN npm install redis
RUN npm install request
RUN npm install body-parser
RUN npm install mathjs
RUN npm install redis-scripto
RUN npm install --save @google-cloud/storage
RUN mkdir -p /app
COPY * /app/
EXPOSE 8080
CMD ["node", "/app/main.js"]





