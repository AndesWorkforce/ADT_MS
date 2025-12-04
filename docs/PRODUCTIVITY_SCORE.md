# Cálculo de Productivity Score - Fórmula Multi-Factor

## 📋 Resumen

El **Productivity Score** es un indicador de 0-100 que mide la productividad de un contractor basándose en múltiples factores:

- Tiempo activo vs idle
- Intensidad de inputs (teclado/mouse)
- Tipo de aplicaciones usadas (productivas vs no productivas)
- Uso de navegación web (dominios productivos vs distractores)

---

## 🧮 Fórmula General

El score final es una **combinación ponderada de 4 sub-scores**, cada uno normalizado a 0-100:

```
productivity_score =
  w1 * S_active +      // 35% - Tiempo activo vs idle
  w2 * S_inputs +      // 20% - Intensidad de inputs
  w3 * S_apps +        // 30% - Apps productivas
  w4 * S_browser       // 15% - Web productiva

// Normalizado a 0-100
productivity_score = min(100, max(0, productivity_score))
```

**Pesos iniciales sugeridos:**

- `w1 = 0.35` (tiempo activo - base fundamental)
- `w2 = 0.20` (intensidad inputs)
- `w3 = 0.30` (apps productivas)
- `w4 = 0.15` (web productiva)

---

## 📊 Sub-Scores Detallados

### 1. S_active: Tiempo Activo vs Idle

**Fórmula:**

```
S_active = 100 * (active_beats / total_beats)
```

**Donde:**

- `active_beats` = cantidad de beats donde `is_idle = false`
- `total_beats` = total de beats en el período (día/sesión)

**Ejemplo:**

- 1000 beats totales
- 700 beats activos, 300 idle
- `S_active = 100 * (700/1000) = 70`

**Interpretación:** Mide el porcentaje de tiempo que el contractor estuvo activo (no idle) durante el período.

---

### 2. S_inputs: Intensidad de Inputs (suavizado actual)

**Fórmula:**

```
inputs_per_minute = (total_keyboard_inputs + total_mouse_clicks) / minutes

// Función logarítmica suavizada para evitar saturación
S_inputs = min(100, 15 * log(1 + inputs_per_minute / 2))

// Opción B: Función por umbrales (más controlable)
if inputs_per_minute < 10:
  S_inputs = 0
elif inputs_per_minute < 50:
  S_inputs = 50 * (inputs_per_minute / 50)
elif inputs_per_minute < 200:
  S_inputs = 50 + 30 * ((inputs_per_minute - 50) / 150)
else:
  S_inputs = 80 + 20 * min(1, (inputs_per_minute - 200) / 300)
```

**Donde:**

- `total_keyboard_inputs` = suma de `keyboard_count` de todos los beats
- `total_mouse_clicks` = suma de `mouse_clicks` de todos los beats
- `minutes` = `(total_beats * 15) / 60` (cada beat = 15 segundos)

**Ejemplo:**

- 5000 keyboard inputs + 2000 mouse clicks = 7000 inputs totales
- 1000 beats = 250 minutos
- `inputs_per_min = 7000 / 250 = 28 inputs/min`
- `S_inputs = 20 * log(1 + 28) ≈ 20 * 3.37 ≈ 67`

**Interpretación:** Mide la intensidad de actividad (cuántos inputs por minuto). Usa función logarítmica para evitar que valores extremos (ej: 1000 inputs/min) dominen el score.

---

### 3. S_apps: Apps Productivas (normalización actual)

**Fórmula:**

```
// Para cada app en AppUsage del período:
weighted_app_seconds = sum(seconds * app_weight for each app)
total_app_seconds = sum(seconds for all apps)

avg_weight = (weighted_app_seconds / total_app_seconds)   // pesos acotados a [0, 1]
S_apps = 100 * clamp((avg_weight - 0.2) / 0.8, 0, 1)
```

**Donde:**

- `app_weight` viene de la tabla `apps_dimension` (ver sección de Tablas de Dimensiones)
- Apps productivas tienen `weight > 1.0`
- Apps neutras tienen `0.5 <= weight < 1.0`
- Apps no productivas tienen `weight < 0.5`

**Ejemplo:**

- AppUsage: `{ "Code": 3600s, "Chrome": 1800s, "YouTube": 600s }`
- Pesos: Code=1.2, Chrome=0.6, YouTube=0.2
- `weighted = (3600*1.2) + (1800*0.6) + (600*0.2) = 4320 + 1080 + 120 = 5520`
- `total = 3600 + 1800 + 600 = 6000`
- `S_apps = 100 * (5520/6000) = 92`

**Interpretación:** Mide qué tan productivas son las aplicaciones que usa el contractor. Tiempo en apps productivas (IDE, Office) suma más que tiempo en apps distractoras (YouTube, juegos).

---

### 4. S_browser: Navegación Web Productiva (normalización actual)

**Fórmula:**

```
// Similar a apps:
weighted_web_seconds = sum(seconds * domain_weight for each domain)
total_web_seconds = sum(seconds for all domains)

avg_weight = (weighted_web_seconds / total_web_seconds)   // pesos acotados a [0, 1]
S_browser = 100 * clamp((avg_weight - 0.2) / 0.8, 0, 1)
```

**Donde:**

- `domain_weight` viene de la tabla `domains_dimension` (ver sección de Tablas de Dimensiones)
- Dominios productivos tienen `weight > 1.0` (ej: github.com, stackoverflow.com)
- Dominios neutros tienen `0.5 <= weight < 1.0` (ej: google.com)
- Dominios no productivos tienen `weight < 0.5` (ej: youtube.com, redes sociales)

**Ejemplo:**

- Browser: `{ "github.com": 1200s, "youtube.com": 600s }`
- Pesos: github.com=1.3, youtube.com=0.2
- `weighted = (1200*1.3) + (600*0.2) = 1560 + 120 = 1680`
- `total = 1200 + 600 = 1800`
- `S_browser = 100 * (1680/1800) = 93`

**Interpretación:** Mide qué tan productivo es el uso de navegación web. Tiempo en dominios productivos (GitHub, Stack Overflow, Jira) suma más que tiempo en distractores (YouTube, redes sociales).

---

## 📋 Tablas de Dimensiones

### apps_dimension

Tabla de lookup que define el peso/productividad de cada aplicación.

**Estructura:**

```sql
CREATE TABLE apps_dimension (
  app_name String,
  category String,  -- 'productive', 'neutral', 'non_productive'
  weight Float64,   -- 0.0 - 2.0 (1.0 = neutro, >1.0 = productivo, <1.0 = no productivo)
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY app_name;
```

**Valores iniciales sugeridos:**

| app_name           | category       | weight | Descripción                 |
| ------------------ | -------------- | ------ | --------------------------- |
| Code               | productive     | 1.2    | IDE genérico                |
| Visual Studio Code | productive     | 1.2    | IDE                         |
| IntelliJ           | productive     | 1.2    | IDE                         |
| Word               | productive     | 1.0    | Office                      |
| Excel              | productive     | 1.0    | Office                      |
| PowerPoint         | productive     | 1.0    | Office                      |
| Slack              | neutral        | 0.8    | Comunicación                |
| Teams              | neutral        | 0.8    | Comunicación                |
| Chrome             | neutral        | 0.6    | Navegador (depende del uso) |
| Edge               | neutral        | 0.6    | Navegador                   |
| YouTube            | non_productive | 0.2    | Entretenimiento             |
| Spotify            | non_productive | 0.3    | Música                      |
| Discord            | non_productive | 0.4    | Chat social                 |
| Games              | non_productive | 0.1    | Juegos                      |
| default            | neutral        | 0.5    | Apps desconocidas           |

---

### domains_dimension

Tabla de lookup que define el peso/productividad de cada dominio web.

**Estructura:**

```sql
CREATE TABLE domains_dimension (
  domain String,
  category String,  -- 'productive', 'neutral', 'non_productive'
  weight Float64,   -- 0.0 - 2.0
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY domain;
```

**Valores iniciales sugeridos:**

| domain            | category       | weight | Descripción           |
| ----------------- | -------------- | ------ | --------------------- |
| github.com        | productive     | 1.3    | Desarrollo            |
| stackoverflow.com | productive     | 1.2    | Desarrollo            |
| atlassian.net     | productive     | 1.1    | Herramientas trabajo  |
| jira.\*           | productive     | 1.1    | Gestión proyectos     |
| confluence.\*     | productive     | 1.1    | Documentación         |
| docs.google.com   | productive     | 1.0    | Documentos            |
| notion.so         | productive     | 1.0    | Notas                 |
| google.com        | neutral        | 0.7    | Búsqueda              |
| bing.com          | neutral        | 0.7    | Búsqueda              |
| extensions        | neutral        | 0.5    | Extensiones navegador |
| youtube.com       | non_productive | 0.2    | Entretenimiento       |
| facebook.com      | non_productive | 0.1    | Red social            |
| twitter.com       | non_productive | 0.2    | Red social            |
| instagram.com     | non_productive | 0.1    | Red social            |
| reddit.com        | non_productive | 0.3    | Foros                 |
| default           | neutral        | 0.5    | Dominios desconocidos |

---

## 📐 Ejemplo de Cálculo Completo

**Datos de entrada (un día):**

- Total beats: 1000
- Active beats: 700
- Keyboard inputs: 5000
- Mouse clicks: 2000
- AppUsage: `{ "Code": 3600s, "Chrome": 1800s, "YouTube": 600s }`
- Browser: `{ "github.com": 1200s, "youtube.com": 600s }`

**Cálculo paso a paso:**

```
1. S_active:
   S_active = 100 * (700/1000) = 70

2. S_inputs:
   inputs_per_min = (5000 + 2000) / (1000*15/60) = 7000 / 250 = 28 inputs/min
   S_inputs = 20 * log(1 + 28) ≈ 20 * 3.37 ≈ 67

3. S_apps:
   weighted_app = (3600*1.2) + (1800*0.6) + (600*0.2) = 5520
   total_app = 6000
   S_apps = 100 * (5520/6000) = 92

4. S_browser:
   weighted_web = (1200*1.3) + (600*0.2) = 1680
   total_web = 1800
   S_browser = 100 * (1680/1800) = 93

5. Score final:
   productivity_score = 0.35*70 + 0.20*67 + 0.30*92 + 0.15*93
                      = 24.5 + 13.4 + 27.6 + 13.95
                      = 79.45%
```

---

## ⚙️ Calibración y Ajustes

### Ajustar Pesos (w1, w2, w3, w4)

Los pesos pueden ajustarse según:

- **Rol del contractor**: Desarrolladores pueden tener más peso en `S_apps` (IDEs), soporte más en `S_browser` (herramientas web).
- **Cliente**: Diferentes clientes pueden tener diferentes definiciones de "productividad".
- **Feedback del negocio**: Validar con muestras reales y ajustar hasta que el score refleje la realidad.

### Ajustar Tablas de Dimensiones

- **Agregar nuevas apps/dominios**: Cuando aparezcan apps/dominios nuevos, agregarlos a las tablas con un peso inicial conservador (0.5).
- **Revisar pesos periódicamente**: Analizar correlación entre tiempo en apps/dominios y productividad real, ajustar pesos.

### Ajustar Función de Inputs

- **Umbrales**: Si la función logarítmica no refleja bien la realidad, usar la función por umbrales y ajustar los puntos de corte.
- **Normalización por rol**: Diferentes roles pueden tener diferentes "inputs normales" (ej: un desarrollador puede tener más inputs que un diseñador).

---

## 🎯 Ventajas de este Enfoque

1. **Explicable**: Cada componente es claro y auditable.
2. **Calibrable**: Fácil ajustar pesos y categorías según necesidad.
3. **Escalable**: Fácil agregar más factores sin romper lo existente.
4. **Contextualizable**: Diferentes pesos por rol/cliente si hace falta.
5. **Basado en datos reales**: Usa todos los datos disponibles del payload definitivo.

---

## 📝 Notas de Implementación

- Los transformers (`ActivityToDailyMetricsTransformer`, `ActivityToSessionSummaryTransformer`) implementan esta lógica.
- Las tablas de dimensiones (`apps_dimension`, `domains_dimension`) deben poblarse antes de calcular scores.
- Para apps/dominios desconocidos, se usa el peso `default` (0.5).
- El score se calcula tanto a nivel diario (`contractor_daily_metrics`) como a nivel sesión (`session_summary`).
