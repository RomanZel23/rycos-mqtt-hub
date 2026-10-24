# RYCOS MQTT Fiscal Hub Daemon

Lekki demon pośredniczący (proxy / load balancer) działający bezpośrednio na poziomie protokołu MQTT. Skrypt przechwytuje zlecenia wysyłane na wirtualne adresy hubów (`rycos/SBF-XXXXXX/command`) i inteligentnie rozdziela je na aktywne urządzenia fizyczne (`rycos/SBR-YYYYYY/command`), a po otrzymaniu odpowiedzi przekazuje ją z powrotem na topik wirtualnego huba.

## Jak to działa?

1. **Adresowanie**: Klient (system POS/ERP) publikuje komendę na topik `rycos/SBF-XXXXXX/command`. `XXXXXX` to pierwsze 8 znaków UUID klienta (pierwszy człon przed myślnikiem).
2. **Dopasowanie klienta**: Demon automatycznie wyszukuje klienta (`clients`) w bazie Supabase na podstawie prefiksu UUID.
3. **Filtrowanie urządzeń**: Demon pobiera z Supabase urządzenia sparowane z tym klientem (`client_id`, `seat_id` != null) i wybiera tylko te, które:
   - Są online (aktywność `last_seen_at` z ostatnich 24 h),
   - Posiadają licencję `RYCOS_F` lub `RYCOS_PF`,
   - Mają działający backend fiskalny:
     - kasa wirtualna Aplikasa na Androidzie (`device_info.companion_apps.aplikasa.installed = true`, urządzenie widziane w ciągu 24 h), albo
     - kasa/drukarka POSNET obsługiwana przez `posnet-agent` (`device_info.fiscal_backend = "posnet"`): agent raportował stan w ciągu `POSNET_MAX_SILENCE_MS` (domyślnie 3 min) i kasa mu odpowiada (`device_info.posnet.online = true`).
4. **Inteligentny Load Balancing**:
   - **Busy State Tracking**: Demon śledzi w pamięci liczbę aktywnych, wiszących zapytań do każdego urządzenia `SBT-YYYYYY`. Pierwszeństwo mają urządzenia z liczbą aktywnych zapytań równą 0 (fizyczna kasa przetwarza wydruki sekwencyjnie).
   - **Least Connections**: Jeśli wszystkie kasy aktualnie drukują, zlecenie trafia do urządzenia o najmniejszej liczbie zapytań w kolejce.
   - **Najświeższy status**: W przypadku remisu, preferowane jest urządzenie o najnowszym statusie `last_seen_at`.
5. **Proxy i sprzątanie**: Demon wysyła komendę na topik fizyczny, tymczasowo śledząc ID transakcji. Po odebraniu odpowiedzi przekazuje ją z powrotem do klienta na topik `rycos/SBF-XXXXXX/response`. Jeśli urządzenie nie odpowie w określonym czasie (`timeout`), demon odsyła błąd 504 i zdejmuje obciążenie z urządzenia.

---

## Wdrożenie na serwerze DigitalOcean Droplet

### Wymagania wstępne
- Zainstalowane środowisko **Node.js** (rekomendowana wersja 18 lub nowsza).
- Dostęp sieciowy do bazy Supabase oraz brokera MQTT.

### Krok 1: Kopiowanie plików
Skopiuj cały katalog `scripts/mqtt-hub` na serwer Droplet (np. do katalogu `/opt/rycos-mqtt-fiscal-hub`).

### Krok 2: Instalacja zależności
W katalogu ze skryptem na serwerze uruchom:
```bash
npm install
```

### Krok 3: Konfiguracja zmiennych środowiskowych
Skopiuj plik przykładowy `.env.example` jako `.env` i uzupełnij poprawne dane:
```bash
cp .env.example .env
nano .env
```

Zabezpiecz plik z poufnymi kluczami przed dostępem innych użytkowników systemu:
```bash
chmod 600 .env
```

### Krok 4: Uruchomienie w tle przez PM2
Do zarządzania procesem w tle na serwerach produkcyjnych najlepiej użyć narzędzia `pm2`.

1. Zainstaluj `pm2` globalnie (jeśli jeszcze nie jest zainstalowane):
   ```bash
   sudo npm install -g pm2
   ```

2. Uruchom demona:
   ```bash
   pm2 start hub.js --name "rycos-mqtt-fiscal-hub"
   ```

3. Ustaw automatyczny start `pm2` wraz z uruchomieniem systemu:
   ```bash
   pm2 startup
   pm2 save
   ```

### Przydatne komendy PM2
- Podgląd statusu usługi: `pm2 status`
- Podgląd logów na żywo: `pm2 logs rycos-mqtt-fiscal-hub`
- Restart usługi: `pm2 restart rycos-mqtt-fiscal-hub`
- Zatrzymanie: `pm2 stop rycos-mqtt-fiscal-hub`

---

## Konfiguracja ACL na Brokerze MQTT (np. Mosquitto)

Upewnij się, że użytkownik, z którego korzysta demon (`MQTT_USER`), posiada w pliku konfiguracji ACL brokera uprawnienia do:
- Subskrybowania (`read`) tematów: `rycos/SBF-+/command` oraz `rycos/+/response`
- Publikowania (`write`) tematów: `rycos/+/command` oraz `rycos/SBF-+/response`

Przykład dla `/etc/mosquitto/acl`:
```text
user rycos_portal
topic readwrite rycos/#
```
*(Lub bardziej restrykcyjne reguły w zależności od polityki bezpieczeństwa)*.
