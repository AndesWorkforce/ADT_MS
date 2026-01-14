/**
 * Script simple para hacer PING a Redis
 *
 * Uso: pnpm ts-node -r tsconfig-paths/register scripts/ping-redis.ts
 */

import 'dotenv/config';
import { createClient } from 'redis';
import { envs } from '../config';

async function pingRedis() {
  console.log('🔍 Haciendo PING a Redis...\n');
  console.log(`📍 Configuración:`);
  console.log(`   Host: ${envs.redis.host}`);
  console.log(`   Port: ${envs.redis.port}`);
  console.log(`   Database: ${envs.redis.db}\n`);

  // Crear cliente Redis
  const client = createClient({
    socket: {
      host: envs.redis.host,
      port: envs.redis.port,
    },
    password: envs.redis.password || undefined,
    database: envs.redis.db,
  });

  // Manejar errores
  client.on('error', (err) => {
    console.error('❌ Error de conexión:', err.message);
    process.exit(1);
  });

  try {
    // Conectar
    console.log('🔌 Conectando...');
    await client.connect();
    console.log('✅ Conectado\n');

    // Hacer PING
    console.log('📤 Enviando PING...');
    const response = await client.ping();
    console.log(`📥 Respuesta: ${response}\n`);

    if (response === 'PONG') {
      console.log(
        '✅ ¡Conexión exitosa! Redis está respondiendo correctamente.\n',
      );

      // Opcional: obtener info del servidor
      try {
        const info = await client.info('server');
        const infoString = String(info);
        const version = infoString.match(/redis_version:([^\r\n]+)/)?.[1];
        if (version) {
          console.log(`ℹ️  Versión de Redis: ${version}`);
        }
      } catch {
        // Ignorar error al obtener info, no es crítico
      }
    } else {
      console.log('⚠️  Respuesta inesperada:', response);
    }
  } catch (error) {
    if (error instanceof Error) {
      console.error('\n❌ Error:', error.message);

      if (error.message.includes('ECONNREFUSED')) {
        console.error('\n💡 El servidor rechazó la conexión. Posibles causas:');
        console.error('   - El puerto está bloqueado por firewall');
        console.error('   - Redis no está escuchando en ese puerto');
        console.error('   - La configuración de puerto es incorrecta');
      } else if (error.message.includes('ENOTFOUND')) {
        console.error(
          '\n💡 No se puede resolver el hostname. Posibles causas:',
        );
        console.error('   - El hostname es incorrecto');
        console.error(
          '   - El hostname es interno y no accesible desde tu red',
        );
      } else if (
        error.message.includes('NOAUTH') ||
        error.message.includes('invalid password')
      ) {
        console.error('\n💡 Error de autenticación. Posibles causas:');
        console.error('   - La contraseña es incorrecta');
        console.error(
          '   - Redis requiere autenticación pero no se proporcionó password',
        );
      }
    } else {
      console.error('❌ Error desconocido:', error);
    }
    process.exit(1);
  } finally {
    // Cerrar conexión
    await client.quit();
    console.log('\n🔌 Conexión cerrada');
  }
}

pingRedis();
