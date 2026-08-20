# rycos-mqtt-fiscal-hub — demon pośredniczący MQTT
#
# Repozytorium: github.com/RomanZel23/rycos-mqtt-hub
# W Coolify: Base directory = /, Build Pack = Dockerfile, bez domeny i bez portów.
#
# Usługa nie wystawia żadnego portu — łączy się wyłącznie wychodząco
# do brokera MQTT (rycos.eu:8883) oraz do Supabase.

FROM node:22-alpine

WORKDIR /app

# Repozytorium zawiera package-lock.json, więc npm ci — build jest powtarzalny
# i nie podmieni wersji zależności między wdrożeniami.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY hub.js ./

ENV NODE_ENV=production

# Demon nie ma endpointu HTTP; o żywotność dba restart policy Coolify.
CMD ["node", "hub.js"]
