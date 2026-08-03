FROM node:20-alpine

# Create app directory
WORKDIR /app

ENV PORT=3010

# Install app dependencies
# package*.json is copied; npm install works without a committed package-lock.json
# Once a package-lock.json is added, switch this back to npm ci for faster builds
COPY package*.json ./
RUN npm install --omit=dev

# Bundle app source
COPY . .

EXPOSE 3010

CMD ["node", "server.js"]
