FROM oven/bun:1

WORKDIR /app

# Copy package files
COPY package.json ./
RUN bun install

# Copy application code
COPY . .

EXPOSE 3000

CMD ["bun", "run", "index.ts"]
