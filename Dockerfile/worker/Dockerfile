#worker image
FROM node:alpine AS worker
ENV PROJECT_ID dta-beta-198020
ENV BUCKET_NAME eltod-1
RUN npm install async
RUN npm install fast-csv
RUN npm install ioredis
RUN npm install --save @google-cloud/storage
RUN mkdir -p /app
COPY worker.js /app/
RUN mkdir /output
VOLUME /output
EXPOSE 8080
CMD ["node", "/app/worker.js"]