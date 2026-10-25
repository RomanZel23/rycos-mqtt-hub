require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const mqtt = require('mqtt');

// ─── Walidacja Zmiennych Środowiskowych ──────────────────────────────────────────
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const mqttHost = process.env.MQTT_HOST || 'rycos.eu';
const mqttPort = parseInt(process.env.MQTT_PORT || '8883', 10);
const mqttUser = process.env.MQTT_USER;
const mqttPass = process.env.MQTT_PASS;
const debugMode = process.env.DEBUG === 'true';
const defaultTimeoutMs = parseInt(process.env.DEFAULT_TIMEOUT_MS || '30000', 10);

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('[Error] Brak SUPABASE_URL lub SUPABASE_SERVICE_ROLE_KEY w zmiennych środowiskowych.');
  process.exit(1);
}

if (!mqttUser || !mqttPass) {
  console.error('[Error] Brak MQTT_USER lub MQTT_PASS w zmiennych środowiskowych.');
  process.exit(1);
}

// Logowanie pomocnicze
function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function debug(...args) {
  if (debugMode) {
    log('[DEBUG]', ...args);
  }
}

// ─── Inicjalizacja Supabase i MQTT ───────────────────────────────────────────────
log('Inicjalizacja klienta Supabase...');
const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: { persistSession: false }
});

const mqttOpts = {
  host: mqttHost,
  port: mqttPort,
  protocol: mqttPort === 8883 ? 'mqtts' : 'mqtt',
  username: mqttUser,
  password: mqttPass,
  clientId: `mqtt-fiscal-hub-${Math.random().toString(36).slice(2, 10)}`,
  clean: true,
  connectTimeout: 15000,
  reconnectPeriod: 5000,
  keepalive: 60,
};

log(`Łączenie z brokerem MQTT ${mqttOpts.protocol}://${mqttHost}:${mqttPort}...`);
const client = mqtt.connect(mqttOpts);

// ─── Pamięć Stanu i Cache ────────────────────────────────────────────────────────
// Cache w pamięci dla redukcji opóźnień i eliminacji wyścigów (race conditions)
const companyCache = new Map(); // prefix -> companyId
const companyDevicesCache = new Map(); // companyId -> { devices: [...], fetchedAt: timestamp }
const activeFetches = new Map(); // prefix -> Promise
const DEVICE_CACHE_TTL_MS = 60000; // 1 minuta

// Kasy POSNET obsługiwane przez posnet-agent (rycos-posnet): agent raportuje stan co STATUS_INTERVAL_S (60 s).
// Dłuższa cisza = agent nie działa albo stracił kasę → nie kierujemy do niej paragonów.
const POSNET_MAX_SILENCE_MS = parseInt(process.env.POSNET_MAX_SILENCE_MS || '180000', 10);

function isPosnetDevice(d) {
  const info = d.device_info || {};
  const apps = info.companion_apps || {};
  return info.fiscal_backend === 'posnet' || !!(apps.posnet && apps.posnet.installed !== undefined);
}

// Mapa aktywnych transakcji: cmdId -> { originalHubTopic, timer, targetDeviceDisplayId }
const pendingTransactions = new Map();

// Mapa liczby aktywnych zapytań per urządzenie: displayId -> liczba
const deviceActiveRequests = new Map();

// Helper do bezpiecznego dekrementowania liczby aktywnych zapytań
function decrementDeviceRequests(displayId) {
  const current = deviceActiveRequests.get(displayId) || 0;
  if (current > 1) {
    deviceActiveRequests.set(displayId, current - 1);
  } else {
    deviceActiveRequests.delete(displayId);
  }
  debug(`Zmniejszono licznik żądań dla ${displayId}. Aktywne: ${deviceActiveRequests.get(displayId) || 0}`);
}

// Funkcja pobierająca i cache'ująca aktywne urządzenia fiskalne dla danego huba
async function getHubAndFiscalDevices(hubDisplayId) {
  // 1. Sprawdź cache dla huba
  const cached = companyDevicesCache.get(hubDisplayId);
  const now = Date.now();
  if (cached && (now - cached.fetchedAt < DEVICE_CACHE_TTL_MS)) {
    debug(`Zwrócono ${cached.devices.length} urządzeń dla huba ${hubDisplayId} z pamięci cache (TTL ok).`);
    return cached.devices;
  }

  // Pobierz urządzenia powiązane z tym hubem (INNER JOIN z fiscal_hubs na podstawie display_id)
  // Wybieramy tylko urządzenia sparowane (seat_id != null)
  const { data: devices, error: devErr } = await supabase
    .from('devices')
    .select('id, client_id, display_id, last_seen_at, device_info, fiscal_hubs!inner(display_id)')
    .eq('fiscal_hubs.display_id', hubDisplayId)
    .not('seat_id', 'is', null);

  if (devErr || !devices || devices.length === 0) {
    if (devErr) {
      console.error(`[Error] Błąd pobierania urządzeń dla huba ${hubDisplayId}:`, devErr.message);
    }
    return [];
  }

  // Pobierz tier licencji per urządzenie (license_seats.device_id → tier)
  const deviceIds = devices.map(d => d.id);
  const { data: seats, error: seatsErr } = await supabase
    .from('license_seats')
    .select('device_id, tier')
    .in('device_id', deviceIds);

  if (seatsErr) {
    console.error('[Error] Błąd pobierania licencji z Supabase:', seatsErr.message);
    if (cached) return cached.devices;
    throw new Error(`Błąd weryfikacji licencji: ${seatsErr.message}`);
  }

  // Filtruj: licencja RYCOS_F/PF i działający backend fiskalny:
  //  • Aplikasa (Android): companion_apps.aplikasa.installed, urządzenie widziane w ciągu 24 h
  //  • POSNET (posnet-agent): device_info.fiscal_backend = "posnet" — agent raportuje co minutę,
  //    więc wymagamy świeżego raportu (POSNET_MAX_SILENCE_MS) i kasy odpowiadającej agentowi (posnet.online)
  const rejected = [];
  const activeFiscalDevices = devices.filter(d => {
    const why = (reason) => { rejected.push(`${d.display_id}: ${reason}`); return false; };
    const seat = seats?.find(s => s.device_id === d.id);
    const tierUpper = seat?.tier?.toUpperCase();
    const isFiscalTier = tierUpper === 'RYCOS_F' || tierUpper === 'RYCOS_PF';
    if (!isFiscalTier) return why(`licencja ${seat?.tier || 'brak'} (wymagana rycos_f/rycos_pf)`);

    const lastSeenMs = d.last_seen_at ? Date.now() - new Date(d.last_seen_at).getTime() : Infinity;

    if (isPosnetDevice(d)) {
      const posnet = (d.device_info && d.device_info.posnet) || {};
      if (lastSeenMs >= POSNET_MAX_SILENCE_MS) return why(`POSNET: brak raportu agenta od ${Math.round(lastSeenMs / 1000)} s (limit ${POSNET_MAX_SILENCE_MS / 1000} s)`);
      if (posnet.online !== true) return why(`POSNET: agent nie łączy się z kasą (${posnet.last_error || 'online=false'})`);
      return true;
    }

    if (lastSeenMs >= 24 * 60 * 60 * 1000) return why('nieaktywne od ponad 24 h');
    const apps = d.device_info && d.device_info.companion_apps;
    const hasAplikasa = !!(apps && apps.aplikasa && apps.aplikasa.installed);
    return hasAplikasa || why('brak Aplikasy');
  });
  if (rejected.length) log(`[Hub ${hubDisplayId}] pominięte urządzenia: ${rejected.join('; ')}`);

  // Zapisz do cache
  companyDevicesCache.set(hubDisplayId, {
    devices: activeFiscalDevices,
    fetchedAt: now
  });

  return activeFiscalDevices;
}

// Zabezpieczenie przed wyścigami: deduplikacja równoległych zapytań o urządzenia dla tego samego huba
function getHubAndFiscalDevicesDeduplicated(hubDisplayId) {
  let activePromise = activeFetches.get(hubDisplayId);
  if (!activePromise) {
    activePromise = getHubAndFiscalDevices(hubDisplayId).finally(() => {
      activeFetches.delete(hubDisplayId);
    });
    activeFetches.set(hubDisplayId, activePromise);
  }
  return activePromise;
}

// ─── Obsługa Połączenia MQTT ─────────────────────────────────────────────────────
client.on('connect', () => {
  log('✓ Połączono pomyślnie z brokerem MQTT.');

  // Subskrybujemy ogólny topik komend, aby przechwytywać zlecenia na wirtualne huby
  // W kodzie obsłużymy tylko te, których identyfikator zaczyna się od SBF-
  const hubTopic = 'rycos/+/command';
  client.subscribe(hubTopic, { qos: 1 }, (err) => {
    if (err) {
      console.error(`[Error] Błąd subskrypcji ${hubTopic}:`, err.message);
      process.exit(1);
    }
    log(`✓ Zasubskrybowano topik komend: ${hubTopic}`);
  });

  // Subskrypcja odpowiedzi fizycznych urządzeń
  const responseTopic = 'rycos/+/response';
  client.subscribe(responseTopic, { qos: 1 }, (err) => {
    if (err) {
      console.error(`[Error] Błąd subskrypcji ${responseTopic}:`, err.message);
      process.exit(1);
    }
    log(`✓ Zasubskrybowano topik odpowiedzi urządzeń: ${responseTopic}`);
  });
});

client.on('error', (err) => {
  console.error('[Error] Błąd klienta MQTT:', err.message);
});

client.on('close', () => {
  log('Połączenie MQTT zostało zamknięte. Próba ponownego połączenia...');
});

// ─── Obsługa Przychodzących Wiadomości ───────────────────────────────────────────
client.on('message', async (topic, messageBuffer) => {
  const rawPayload = messageBuffer.toString('utf-8');
  
  // 1. Obsługa odpowiedzi z urządzenia fizycznego (rycos/SBT-XXXXXX/response)
  const responseMatch = topic.match(/^rycos\/([^/]+)\/response$/);
  if (responseMatch) {
    const sbtDisplayId = responseMatch[1];
    
    // Ignoruj odpowiedzi wirtualnych hubów (na wypadek gdyby trafiły do rycos/+/response)
    if (sbtDisplayId.startsWith('SBF-')) return;

    try {
      const response = JSON.parse(rawPayload);
      const cmdId = response.id;
      
      if (!cmdId) return;

      const pending = pendingTransactions.get(cmdId);
      if (pending) {
        debug(`Odebrano odpowiedź z ${sbtDisplayId} dla transakcji ${cmdId}`);
        
        // Wyczyść timer timeoutu
        clearTimeout(pending.timer);
        
        // Usuń z mapy transakcji
        pendingTransactions.delete(cmdId);
        
        // Zmniejsz licznik aktywnych zapytań dla urządzenia
        decrementDeviceRequests(pending.targetDeviceDisplayId);

        // Wyślij odpowiedź z powrotem na wirtualny hub (rycos/SBF-XXXXXX/response)
        const responseTopic = `${pending.originalHubTopic.replace('/command', '')}/response`;
        
        // Dodaj display_id urządzenia, które faktycznie obsłużyło zapytanie, dla celów statystyk/logów
        const decoratedResponse = {
          ...response,
          // posnet-agent sam podaje kasę, która wystawiła paragon (np. ten sam externalrefFR wysłany wcześniej na inną kasę)
          target_display_id: response.target_display_id || sbtDisplayId
        };

        client.publish(responseTopic, JSON.stringify(decoratedResponse), { qos: 1 }, (pubErr) => {
          if (pubErr) {
            console.error(`[Error] Błąd publikowania odpowiedzi na ${responseTopic}:`, pubErr.message);
          } else {
            debug(`✓ Przekazano odpowiedź do ${responseTopic}`);
          }
        });
      }
    } catch (err) {
      console.error('[Error] Błąd parsowania odpowiedzi z urządzenia:', err.message);
    }
    return;
  }

  // 2. Obsługa komendy na wirtualny hub (rycos/SBF-YYYYYY/command)
  const hubMatch = topic.match(/^rycos\/(SBF-[^/]+)\/command$/);
  if (hubMatch) {
    const hubDisplayId = hubMatch[1];
 
    log(`Odebrano zlecenie na wirtualny hub ${hubDisplayId}`);
 
    let command;
    try {
      command = JSON.parse(rawPayload);
    } catch (err) {
      console.error(`[Error] Nieprawidłowy JSON w komendzie dla ${hubDisplayId}:`, err.message);
      return;
    }
 
    const cmdId = command.id || Math.random().toString(36).substring(2, 15);
    const timeoutMs = command.timeout || defaultTimeoutMs;
 
    try {
      // Pobierz aktywne urządzenia fiskalne za pomocą cache i deduplikacji zapytań
      const activeFiscalDevices = await getHubAndFiscalDevicesDeduplicated(hubDisplayId);

      if (!activeFiscalDevices || activeFiscalDevices.length === 0) {
        log(`[Warn] Brak dostępnych urządzeń online z licencją fiskalną w hubie ${hubDisplayId}`);
        sendErrorResponse(hubDisplayId, cmdId, 503, 'Brak dostępnych urządzeń fiskalnych online w hubie');
        return;
      }

      // ─── Inteligentny Load Balancing ───────────────────────────────────────────
      // Wybierz urządzenia o najmniejszej liczbie aktywnych żądań (Busy State Tracking)
      // W przypadku remisu losujemy z grupy o najmniejszym obciążeniu (Randomized Tie-Breaker)
      // Dzięki temu przy sekwencyjnym ruchu (concurrency=1) zlecenia będą rozdzielane statystycznie po równo.
      let candidates = [];
      let minRequests = Infinity;

      for (const dev of activeFiscalDevices) {
        const activeReqs = deviceActiveRequests.get(dev.display_id) || 0;
        if (activeReqs < minRequests) {
          minRequests = activeReqs;
          candidates = [dev];
        } else if (activeReqs === minRequests) {
          candidates.push(dev);
        }
      }

      const selectedDevice = candidates[Math.floor(Math.random() * candidates.length)];

      const targetDisplayId = selectedDevice.display_id;
      
      // Zwiększ licznik zapytań dla wybranego urządzenia
      const currentActive = deviceActiveRequests.get(targetDisplayId) || 0;
      deviceActiveRequests.set(targetDisplayId, currentActive + 1);

      log(`Kierowanie transakcji ${cmdId} do urządzenia ${targetDisplayId} (Aktywne zlecenia: ${currentActive + 1})`);

      // Zapisz transakcję w pamięci z obsługą timeoutu
      const timer = setTimeout(() => {
        const pending = pendingTransactions.get(cmdId);
        if (pending) {
          pendingTransactions.delete(cmdId);
          decrementDeviceRequests(targetDisplayId);
          log(`[Timeout] Brak odpowiedzi od ${targetDisplayId} dla transakcji ${cmdId} po ${timeoutMs}ms`);
          sendErrorResponse(hubDisplayId, cmdId, 504, `Limit czasu MQTT (${timeoutMs}ms) oczekując na odpowiedź od urządzenia ${targetDisplayId}`);
        }
      }, timeoutMs);

      pendingTransactions.set(cmdId, {
        originalHubTopic: topic,
        timer,
        targetDeviceDisplayId: targetDisplayId
      });

      // Opublikuj komendę na topik fizycznego urządzenia
      const targetTopic = `rycos/${targetDisplayId}/command`;
      
      // Zachowaj oryginalną strukturę komendy, ale upewnij się, że ma poprawne ID
      const targetPayload = JSON.stringify({
        ...command,
        id: cmdId
      });

      client.publish(targetTopic, targetPayload, { qos: 1 }, (pubErr) => {
        if (pubErr) {
          console.error(`[Error] Błąd publikowania do ${targetTopic}:`, pubErr.message);
          
          // Sprzątanie po błędzie publikacji
          clearTimeout(timer);
          pendingTransactions.delete(cmdId);
          decrementDeviceRequests(targetDisplayId);
          sendErrorResponse(hubDisplayId, cmdId, 502, `Błąd wysyłania komendy do brokera: ${pubErr.message}`);
        } else {
          debug(`✓ Komenda wysłana na topik ${targetTopic}`);
        }
      });

    } catch (err) {
      console.error(`[Error] Krytyczny błąd obsługi zlecenia w hubie dla ${hubDisplayId}:`, err.message);
      sendErrorResponse(hubDisplayId, cmdId, 500, `Błąd wewnętrzny demona hubu: ${err.message}`);
    }
  }
});

// Helper do wysyłania ujednoliconych błędów do klienta
function sendErrorResponse(hubDisplayId, cmdId, statusCode, errorMessage) {
  const errorTopic = `rycos/${hubDisplayId}/response`;
  const errorResponse = {
    id: cmdId,
    status: statusCode,
    result: { error: errorMessage },
    ts: Date.now()
  };

  client.publish(errorTopic, JSON.stringify(errorResponse), { qos: 1 }, (err) => {
    if (err) {
      console.error(`[Error] Błąd wysyłania odpowiedzi o błędzie na ${errorTopic}:`, err.message);
    } else {
      debug(`Wysłano odpowiedź o błędzie (${statusCode}) na ${errorTopic}`);
    }
  });
}

// Graceful Shutdown
process.on('SIGINT', () => {
  log('Zamykanie demona MQTT Fiscal Hub (SIGINT)...');
  
  // Wyczyść wszystkie timery transakcji
  for (const [cmdId, pending] of pendingTransactions) {
    clearTimeout(pending.timer);
  }
  
  client.end(true, {}, () => {
    log('Rozłączono z MQTT. Wyjście.');
    process.exit(0);
  });
});
