FROM node:24-slim

ENV NODE_ENV=production

WORKDIR /app

COPY backend/package.json backend/package-lock.json ./backend/
RUN cd backend && npm ci --omit=dev

COPY backend/src ./backend/src
COPY frontend ./frontend

USER node

WORKDIR /app/backend

EXPOSE 3000

CMD ["node", "src/server.js"]
