FROM node:18-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm config set strict-ssl false && npm install --omit=dev
COPY . .
CMD ["npm", "start"]
