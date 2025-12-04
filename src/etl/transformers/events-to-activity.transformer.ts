import { Injectable } from '@nestjs/common';

import { EventRawDto } from '../../raw/dto/event-raw.dto';
import { ContractorActivity15sDto } from '../dto/contractor-activity-15s.dto';

/**
 * Transforma eventos RAW (events_raw) en beats de 15s para contractor_activity_15s.
 * La lógica está alineada con DATA_WAREHOUSE_ANALYSIS.md:
 * - keyboard_count  ← payload.Keyboard.InputsCount
 * - mouse_clicks   ← payload.Mouse.ClicksCount
 * - is_idle        ← IdleTime > 0
 */
@Injectable()
export class EventsToActivityTransformer {
  transform(event: EventRawDto): ContractorActivity15sDto {
    const beatTimestamp = event.timestamp;

    // El payload llega a RAW como string JSON
    const parsed =
      typeof event.payload === 'string'
        ? JSON.parse(event.payload || '{}')
        : event.payload || {};

    const keyboard = parsed.Keyboard || {};
    const mouse = parsed.Mouse || {};

    const idleTime: number =
      typeof parsed.IdleTime === 'number' ? parsed.IdleTime : 0;

    const dto = new ContractorActivity15sDto();
    dto.contractor_id = event.contractor_id;
    dto.agent_id = event.agent_id ?? null;
    dto.session_id = event.session_id ?? null;
    dto.agent_session_id = event.agent_session_id ?? null;

    dto.beat_timestamp = beatTimestamp;

    dto.keyboard_count =
      typeof keyboard.InputsCount === 'number' ? keyboard.InputsCount : 0;
    dto.mouse_clicks =
      typeof mouse.ClicksCount === 'number' ? mouse.ClicksCount : 0;

    // Lógica para determinar si un beat es idle:
    // Un beat es idle si:
    // 1. El IdleTime es >= 10 segundos (mayor parte del beat de 15s fue idle)
    // 2. Y no hay inputs ni clicks (sin actividad detectable)
    // Un beat es activo si:
    // - Hay inputs o clicks (actividad clara)
    // - O el idle time es < 10 segundos (aunque no haya inputs, podría estar trabajando)
    const IDLE_THRESHOLD_SECONDS = 10; // De 15 segundos del beat
    const hasActivity = dto.keyboard_count > 0 || dto.mouse_clicks > 0;

    // Es idle solo si el idle time es significativo (>= 10s) Y no hay actividad
    dto.is_idle = idleTime >= IDLE_THRESHOLD_SECONDS && !hasActivity;

    return dto;
  }
}
