FROM launcher.gcr.io/google/nodejs
RUN mkdir -p /app
COPY * /app/
RUN npm install async
RUN npm install hashtable
RUN npm install fast-csv
RUN npm install redis
RUN npm install priorityqueuejs
RUN npm install express
RUN npm install body-parser
RUN npm install mathjs
RUN npm install redis-scripto
EXPOSE 8080
CMD ["node", "worker.js"]





