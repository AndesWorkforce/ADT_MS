Con toda esta info, puedo proponerte una estructura sólida para un Data Warehouse en ClickHouse que soporte análisis de productividad, sesiones, actividad del agente, y uso de teclado/mouse/apps.
Voy a darte:
✔️ 1. Principios de modelado en ClickHouse para tu caso
✔️ 2. Tablas recomendadas (RAW → ADT)
✔️ 3. Tablas finales ADT sugeridas
✔️ 4. Métricas que podés sacar fácilmente con esta estructura

---

🧩 1. Principios para tu Warehouse en ClickHouse
ClickHouse funciona mejor cuando:
• Las tablas son append-only
• Los datos están desnormalizados (más columnas, menos joins)
• Las consultas son analíticas, no transaccionales
• Se aprovechan particiones por fecha, contractor, etc.
Vos tenés:
• Usuarios → Administración
• Contractors → Sujetos de medición
• Sessions, AgentSessions → Tiempos
• Event → Actividad granular (teclado/mouse/apps)
El warehouse debe ser una versión agregada y optimizada para consultas de productividad, NO un mirror exacto del modelo SQL transaccional.

---

🏗️ 2. Tablas RAW en ClickHouse (etl inicial)
Estas tablas almacenan los datos “como vienen” desde Postgres, con mínimos cambios:

---

RAW: events_raw
Guarda todos los eventos capturados por el agente (teclado, mouse, apps, navegador, etc.).
Refleja 1:1 lo que envía `EventRawDto` y lo que inserta `RawService.saveEvent`.

CREATE TABLE events_raw (
event_id String,
contractor_id String,
agent_id Nullable(String),
session_id Nullable(String),
agent_session_id Nullable(String),
timestamp DateTime,
payload String, -- JSON string (como viene del EVENTS_MS)
created_at DateTime -- se respeta el valor enviado desde Postgres
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (contractor_id, timestamp);
👉 Esta es la base del 80% de las métricas.

---

RAW: sessions_raw
Refleja 1:1 lo que envía `SessionRawDto` y lo que inserta `RawService.saveSession`.

CREATE TABLE sessions_raw (
session_id String,
contractor_id String,
session_start DateTime,
session_end Nullable(DateTime),
total_duration Nullable(Int32),
created_at DateTime,
updated_at DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(session_start)
ORDER BY (contractor_id, session_start);

---

RAW: agent_sessions_raw
Refleja 1:1 lo que envía `AgentSessionRawDto` y lo que inserta `RawService.saveAgentSession`.

CREATE TABLE agent_sessions_raw (
agent_session_id String,
contractor_id String,
agent_id String,
session_id Nullable(String),
session_start DateTime,
session_end Nullable(DateTime),
total_duration Nullable(Int32),
created_at DateTime,
updated_at DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(session_start)
ORDER BY (contractor_id, session_start);

---

RAW: contractor_info_raw
Esta tabla trae un snapshot enriquecido del contractor, tal como lo construye `ContractorRawDto` y lo inserta `RawService.saveContractor`.

CREATE TABLE contractor_info_raw (
contractor_id String,
name String,
email Nullable(String),
job_position String,
work_schedule_start Nullable(String),
work_schedule_end Nullable(String),
country Nullable(String),
client_id String,
team_id Nullable(String),
created_at DateTime,
updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY contractor_id;

---

🔥 3. Tablas ADT (Analytical Data Tables)
Estas son las tablas que realmente vas a consultar para determinar la productividad.
Son agregaciones diarias o por sesión, diseñadas para BI.

---

⭐ ADT PRINCIPAL: contractor_activity_15s
💡 Esta es tu tabla más importante.
Cada fila representa un "heartbeat" de 15 segundos.
Contiene la actividad procesada desde el payload.
CREATE TABLE contractor_activity_15s (
contractor_id String,
agent_id String,
session_id String,
agent_session_id String,

beat_timestamp DateTime, -- timestamp del heartbeat
is_idle UInt8, -- 1: sin inputs
keyboard_count UInt32,
mouse_clicks UInt32,
mouse_distance Float64,
active_app String,
active_window String,

-- extras calculados
productivity_score Float64, -- definida por vos
workday Date, -- toDate(beat_timestamp)
created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY workday
ORDER BY (contractor_id, beat_timestamp);
Con esta tabla podés sacar:
• productividad por hora/día/sesión
• tiempo activo vs idle
• uso de apps
• actividad por cliente, equipo, país
• anomalías, ociosidad, AFK, patrones de trabajo

---

⭐ ADT: contractor_daily_metrics
Tabla agregada por día/contractor.
CREATE TABLE contractor_daily_metrics (
contractor_id String,
workday Date,

total_beats UInt32,
active_beats UInt32,
idle_beats UInt32,
active_percentage Float64,

total_keyboard_inputs UInt64,
total_mouse_clicks UInt64,
avg_keyboard_per_min Float64,
avg_mouse_per_min Float64,

focus_apps Array(String),
top_app String,

total_session_time_seconds UInt64,
effective_work_seconds UInt64,

productivity_score Float64,

created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree
PARTITION BY workday
ORDER BY (contractor_id, workday);

---

⭐ ADT: app_usage_summary
Analytics por aplicación:
CREATE TABLE app_usage_summary (
contractor_id String,
app_name String,
workday Date,

active_beats UInt32,
idle_beats UInt32,
keyboard_inputs UInt32,
mouse_inputs UInt32,
productivity_contribution Float64,

created_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree
PARTITION BY workday
ORDER BY (contractor_id, app_name, workday);

---

⭐ ADT: session_summary
CREATE TABLE session_summary (
session_id String,
contractor_id String,
session_start DateTime,
session_end DateTime,
total_seconds UInt32,
active_seconds UInt32,
idle_seconds UInt32,
productivity_score Float64
)
ENGINE = MergeTree
PARTITION BY toDate(session_start)
ORDER BY (contractor_id, session_start);

---

📊 4. Métricas que podés sacar con este modelo
✔️ Productividad
• % activo vs % idle
• inputs por minuto
• actividad por app critica
• tiempo en apps no permitidas
• tiempo en focus vs multitasking
✔️ Sesiones
• jornada real vs jornada contratada
• cortes de sesión
• sesiones falsas o manipuladas
✔️ Contratistas
• ranking de productividad por equipo/cliente
• anomalías diarias/ciclos
• tiempo total efectivo
✔️ Agente
• latencia del heartbeat
• problemas de conexión
• cambios de VM/host

Diseñame las 4 cosas, La lista es: ✔️ Los pipelines ETL RAW → ADT ✔️ Los métodos de cálculo de “productividad_score” ✔️ Los queries de ClickHouse para dashboards ✔️ Una arquitectura completa (Agent → API Gateway → Queue → Warehouse)
ChatGPT dijo:
Pensado durante 5s
Perfecto — te doy todo armado y listo para implementar. Uso comillas simples en los snippets tal como preferís. Voy por cada ítem de la lista y te doy código/SQL de ejemplo, pseudocódigo para ETL y una arquitectura con recomendaciones prácticas.

1. Pipelines ETL RAW → ADT (end-to-end)
   Objetivo: tomar events_raw (payload JSON cada 15s) → normalizar / enriquecer → almacenar contractor_activity_15s (heartbeat) → agregaciones diarias (contractor_daily_metrics, app_usage_summary, session_summary).
   Resumen de etapas

1) Ingestión: agente → API → queue (NATS) → consumer.
2) Landing RAW: consumer escribe a events_raw (ClickHouse) o a Postgres Raw (según preferencia).
   o Recomendación: usar ClickHouse como landing directo (Kafka/NATS → ClickHouse Kafka engine OR consumer batch INSERT).
3) Parsing / Normalización: extraer campos del payload JSON en columnas (keyboard_count, mouse_clicks, mouse_distance, active_app, active_window, beat_timestamp, etc.) → insertar en contractor_activity_15s.
4) Enriquecimiento: unir metadatos desde contractor_info_raw (country, team_id, client_id) y agent (host).
5) Deduplicación / ordenación: dedup por contractor_id + beat_timestamp + agent_id.
6) ADT Aggregations: materialized views o jobs diarios para llenar contractor_daily_metrics, app_usage_summary, session_summary.
7) Retención / TTL: RAW: 30–90 días; ADT: conservar según política (p. ej. 1 año) y por compresiones.

---

A — Ingestión (consumer pseudocódigo)
Supongamos NATS → consumer (Node/Python) que escribe a ClickHouse.

# pseudocódigo (python)

from nats import connect
from clickhouse_driver import Client
import json

ch = Client(host='clickhouse-host')
nc = connect(servers=['nats://nats:4222'])

def handle_msg(msg):
data = json.loads(msg.data) # data: { contractor_id, agent_id, session_id, timestamp, payload {...} } # normalize: ensure timestamp is ISO, fill missing fields
row = {
'event_id': data.get('id'),
'contractor_id': data['contractor_id'],
'agent_id': data.get('agent_id'),
'session_id': data.get('session_id'),
'timestamp': data['timestamp'],
'payload': json.dumps(data.get('payload', {})),
'created_at': data.get('created_at')
} # batch insert
ch.execute(
"INSERT INTO events_raw (event_id, contractor_id, agent_id, session_id, timestamp, payload, created_at) VALUES",
[tuple(row.values())]
)

nc.subscribe('agent.events', cb=handle_msg)

---

B — Parsing & normalización directo a ClickHouse
Si guardás RAW en ClickHouse, podés tener una materialized view que extrae campos del JSON a contractor_activity_15s.
Ejemplo materialized view (ClickHouse):
CREATE TABLE events_raw (
event_id String,
contractor_id String,
agent_id String,
session_id String,
agent_session_id String,
timestamp DateTime,
payload String, -- JSON string
created_at DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (contractor_id, timestamp);

CREATE TABLE contractor_activity_15s (
contractor_id String,
agent_id String,
session_id String,
agent_session_id String,
beat_timestamp DateTime,
is_idle UInt8,
keyboard_count UInt32,
mouse_clicks UInt32,
mouse_distance Float64,
active_app String,
active_window String,
workday Date DEFAULT toDate(beat_timestamp),
created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY workday
ORDER BY (contractor_id, beat_timestamp);

CREATE MATERIALIZED VIEW mv_events_to_activity
TO contractor_activity_15s AS
SELECT
contractor_id,
agent_id,
session_id,
JSONExtractString(payload, 'agent_session_id') AS agent_session_id,
timestamp AS beat_timestamp,
-- extraer campos del payload
if(JSONExtractInt(payload, 'keyboard', 0) IS NULL, 0, JSONExtractInt(payload, 'keyboard')) AS keyboard_count,
if(JSONExtractInt(payload, 'mouse_clicks', 0) IS NULL, 0, JSONExtractInt(payload, 'mouse_clicks')) AS mouse_clicks,
if(JSONExtractFloat(payload, 'mouse_distance', 0.0) IS NULL, 0.0, JSONExtractFloat(payload, 'mouse_distance')) AS mouse_distance,
JSONExtractString(payload, 'active_app') AS active_app,
JSONExtractString(payload, 'active_window') AS active_window,
-- calcular is_idle: ejemplo simple
(toUInt8( JSONExtractInt(payload, 'keyboard') = 0 AND JSONExtractInt(payload, 'mouse_clicks') = 0 )) AS is_idle,
now() AS created_at
FROM events_raw;
Ajustá los nombres de campos JSON según el payload real.

---

C — ETL batch para ADT (ejemplo cron/ Airflow job)
• Frecuencia: cada 1–5 minutos para contractor_activity_15s si no usás MVs, o en tiempo real con la MV.
• Diarias (00:05): job que calcula contractor_daily_metrics a partir de contractor_activity_15s haciendo agregaciones por workday.
Ejemplo insert diario:
INSERT INTO contractor_daily_metrics
SELECT
contractor_id,
workday,
count() AS total_beats,
sum(1 - is_idle) AS active_beats,
sum(is_idle) AS idle_beats,
round(100.0 _ sum(1 - is_idle) / count(), 2) AS active_percentage,
sum(keyboard_count) AS total_keyboard_inputs,
sum(mouse_clicks) AS total_mouse_clicks,
-- avg per minute (cada beat es 15s => 4 beats/min)
round(sum(keyboard_count) / (count() / 4.0), 2) AS avg_keyboard_per_min,
round(sum(mouse_clicks) / (count() / 4.0), 2) AS avg_mouse_per_min,
groupArrayMap(x -> x.1, arraySort((x,y)->y.2, groupArray((active_app, countIf(active_app != '', 1)))))[1:5] AS focus_apps,
arrayElement(groupArrayMap(x->x.1, arraySort((x,y)->y.2, groupArray((active_app, countIf(active_app != '', 1))))), 1) AS top_app,
-- total session time approximated: total_beats _ 15
(count() _ 15) AS total_session_time_seconds,
(sum(1 - is_idle) _ 15) AS effective_work_seconds,
0.0 AS productivity_score, -- se actualizará con la función de scoring
now() AS created_at
FROM contractor_activity_15s
WHERE workday = today() - 1
GROUP BY contractor_id, workday;
(Esto crea un snapshot diario; para updates usar ReplacingMergeTree o SummingMergeTree según diseño.)

---

2. Métodos de cálculo de “productividad_score”
   Querés un score que represente productividad por beat/session/día. Te propongo 3 capas: Beat-level, Session-level, Daily-level. El score final es ponderado.
   Principios
   • Normalizar métricas por individuo y por rol (p. ej. desarrollador vs soporte puede tener diferentes patrones).
   • Evitar sesgos por número de beats (normalizar por minutos).
   • Penalizar idle largo, penalizar alternancia frecuente (multitasking).
   • Recompensar foco (uso sostenido de apps productivas).
   Variables base (ejemplos)
   • active_pct = active_beats / total_beats (0..1)
   • inputs_per_min = (keyboard_count + mouse_clicks) / minutes
   • focus_ratio = time_in_top_app / effective_work_seconds (0..1)
   • app_productivity_factor = weight(app) (configurable per-app)
   • session_consistency = 1 - (stddev of activity per 5-min window normalized)
   Fórmula propuesta (simple, interpretable)
   Primero computar sub-scores entre 0 y 100:

1) S_active = 100 \* active_pct
2) S_inputs = min(100, 20 \* log(1 + inputs_per_min)) — evita escalamiento lineal
3) S_focus = 100 \* focus_ratio
4) S_app = 100 \* normalized_app_factor — donde apps productivas tienen factor >1 y no-productivas <1
5) S_consistency = 100 * session_consistency
   Luego combinar con pesos:
   productivity_score = w1*S_active + w2*S_inputs + w3*S_focus + w4*S_app + w5*S_consistency
   Con pesos sugeridos (ajustables):
   • w1 = 0.35
   • w2 = 0.2
   • w3 = 0.2
   • w4 = 0.15
   • w5 = 0.10
   Normalizar a 0..100.
   Ejemplo cálculo en ClickHouse (por día)
   SELECT
   contractor_id,
   workday,
   -- submetrics
   active_pct,
   inputs_per_min,
   focus_ratio,
   app_factor,
   consistency,

-- subscores
(100.0 _ active_pct) AS S_active,
least(100.0, 20.0 _ log(1 + inputs_per_min)) AS S_inputs,
(100.0 _ focus_ratio) AS S_focus,
(100.0 _ app_factor) AS S_app,
(100.0 \* consistency) AS S_consistency,

-- final
round(
0.35 _ (100.0 _ active_pct)

- 0.2 _ least(100.0, 20.0 _ log(1 + inputs_per_min))
- 0.2 _ (100.0 _ focus_ratio)
- 0.15 _ (100.0 _ app_factor)
- 0.1 _ (100.0 _ consistency)
  , 2) AS productivity_score
  FROM (
  SELECT
  contractor_id,
  workday,
  sum(1 - is_idle) / count() AS active_pct,
  (sum(keyboard_count + mouse_clicks) / (count() / 4.0)) AS inputs_per_min,
  -- focus_ratio example: beats where active_app = top_app
  sum(if(active_app = top_app, 1, 0)) _ 1.0 _ 15 / greatest(1, sum(1 - is_idle) \* 15) AS focus_ratio,
  -- app factor: weighted average of app weights (weights table to join)
  avg(app_weight) AS app_factor,
  -- simple consistency: 1 - normalized stddev of active beats per 5-min window
  1 - (stddevPop(active_5min) / greatest(1, avg(active_5min))) AS consistency
  FROM ... -- join / subqueries
  )
  Notas:
  • app_weight la definís en una tabla apps_weights(app_name, weight) (ej: IDE = 1.2, Slack = 0.6).
  • active_5min se calcula agregando is_idle en bloques de 5 minutos.
  Consejos de calibración
  • Empieza con pesos propuestos y valida contra evaluaciones manuales de 50 muestras.
  • Ajustá por rol: aplicar factores por job_position.
  • Añadí outlier detection (p. ej. inputs extremadamente altos).

---

3. Queries de ClickHouse para dashboards
   Te doy queries útiles listos para dashboards (Grafana/Metabase).
   A — Overview diario por contrato/cliente
   SELECT
   c.client_id,
   c.contractor_id,
   sum(total_session_time_seconds) AS total_seconds,
   sum(effective_work_seconds) AS effective_seconds,
   round(100.0 _ sum(effective_work_seconds) / greatest(1, sum(total_session_time_seconds)), 2) AS effective_pct,
   avg(productivity_score) AS avg_productivity
   FROM contractor_daily_metrics c
   WHERE workday BETWEEN today() - 7 AND today() - 1
   GROUP BY c.client_id, c.contractor_id
   ORDER BY avg_productivity DESC
   LIMIT 200;
   B — Trend: productividad por hora (últimas 72h)
   SELECT
   toStartOfHour(beat_timestamp) AS hour,
   round(100.0 _ sum(1 - is_idle) / count(), 2) AS active_pct,
   round(avg(productivity_score),2) AS avg_productivity
   FROM contractor_activity_15s
   WHERE contractor_id = 'CONTRACTOR_123' AND beat_timestamp >= now() - INTERVAL 72 HOUR
   GROUP BY hour
   ORDER BY hour;
   C — Top apps por uso (día)
   SELECT
   active_app,
   sum(1) AS beats,
   sum(keyboard_count) AS keyboard_inputs,
   sum(mouse_clicks) AS mouse_inputs
   FROM contractor_activity_15s
   WHERE workday = today() - 1
   GROUP BY active_app
   ORDER BY beats DESC
   LIMIT 50;
   D — Detail: sesión específica con timeline
   SELECT
   beat_timestamp,
   is_idle,
   keyboard_count,
   mouse_clicks,
   active_app,
   active_window
   FROM contractor_activity_15s
   WHERE session_id = 'SESSION_ABC'
   ORDER BY beat_timestamp;
   E — Anomalías: beats con latencia o payload extraño
   SELECT _
   FROM events_raw
   WHERE JSONHas(payload, 'error') OR JSONExtractString(payload, 'heartbeat_status') = 'late'
   ORDER BY timestamp DESC
   LIMIT 200;
   F — Distribución de productivity_score (histograma)
   SELECT
   intDiv(productivity_score, 5) _ 5 AS bucket,
   count() AS cnt
   FROM contractor_daily_metrics
   WHERE workday BETWEEN today() - 30 AND today() - 1
   GROUP BY bucket
   ORDER BY bucket;

---

4. Arquitectura completa (Agent → API Gateway → Queue → Warehouse)
   Componentes y rol
   • Agent (background): envía heartbeat cada 15s con payload JSON. Firma/crypto opcional para integridad.
   • API Gateway: autenticación (JWT), rate-limit, validación básica. Podés usar Nginx + Kong, o tu microservicio de API-GATEWAY. (Tenés NATS en el stack, buen match).
   • Queue/Streaming: NATS JetStream (ligero, alto throughput) o Kafka si necesitás más ecosistema. NATS es suficiente y ya lo usaste.
   • Ingest Consumer: servicio (Go/Node/Python) que consume desde NATS y hace:
   o Validación mínima
   o Enriquecimiento rápido
   o Escritura a ClickHouse events_raw (batches)
   • Stream Processor / ETL Worker:
   o Puede ser el mismo consumer o un worker separado que lee events_raw y escribe contractor_activity_15s (o usar ClickHouse MV)
   o Recomendación: usar Materialized Views en ClickHouse para parsing en tiempo real + job nightly para daily aggregates
   • ClickHouse: almacenamiento de RAW y ADT, con tablas descritas arriba. Particioná por workday y compresión LZ4.
   • Orquestador (opcional): Airflow / Dagster para jobs diarios/semana/mensuales (backfills, re-calculations).
   • Monitoring / Logging: Prometheus + Grafana para métricas infra, Loki/ELK para logs.
   • BI: Grafana / Metabase / Superset → consultas a ClickHouse.
   Consideraciones operativas
   • Schema registry para payloads (avro/jsonschema) si el payload evoluciona.
   • Idempotencia: agent debe enviar event_id para dedup.
   • Backpressure: si NATS acumula, el consumer debe procesar en batches; ClickHouse acepta inserts masivos mejor que muchos inserts pequeños.
   • Seguridad: TLS entre agent → API Gateway → NATS; RBAC en ClickHouse.
   • Testing: simulá agentes con carga (locust) para validar throughput.
   • Retention: usar TTLs en ClickHouse (ALTER TABLE ... MODIFY TTL ...) para RAW.

---

Extras prácticos / Plan de acción (prioridades)

1. Implementar events_raw + materialized view mv_events_to_activity para tener contractor_activity_15s en tiempo real.
2. Crear tabla apps_weights y definir initial weights.
3. Implementar job diario que calcule contractor_daily_metrics y ejecute scoring.
4. Montar dashboards básicos (overview, trend, top apps).
5. Calibrar productivity_score con muestras reales (ajustar pesos).
6. Añadir alertas (ej. caída de heartbeats, picos de idle).
