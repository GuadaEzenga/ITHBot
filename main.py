import os
import time
import sqlite3
import logging
import requests
import atexit
import signal
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, List, Tuple

load_dotenv()

# Configuración
TOKEN = os.getenv("TOKEN")
API_KEY = os.getenv("API_KEY")
CIUDAD = os.getenv("CIUDAD")
HORAS = ["10:00", "12:00", "16:00"]

DB_PATH = "users.db"
LAST_UPDATE_PATH = "last_update_id.txt"

# Logging mejorado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot_ith.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ITHBot:
    def __init__(self):
        self.validar_config()
        self.init_db()
        self.ultima_ejecucion = None
        self.last_update_id = self.leer_ultimo_update_id()
        
        # Cleanup al salir
        atexit.register(self.cleanup)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    @staticmethod
    def validar_config():
        missing = []
        if not TOKEN: missing.append("TOKEN")
        if not API_KEY: missing.append("API_KEY") 
        if not CIUDAD: missing.append("CIUDAD")
        if missing:
            logger.critical(f"Faltan variables: {', '.join(missing)}")
            raise SystemExit(1)

    @contextmanager
    def get_db_connection(self):
        """Context manager para SQLite seguro"""
        conn = sqlite3.connect(DB_PATH, timeout=10)
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self):
        with self.get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def guardar_usuario(self, chat_id: str):
        with self.get_db_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (chat_id) VALUES (?)", 
                (chat_id,)
            )
            conn.commit()
        logger.info(f"Usuario guardado: {chat_id}")

    def obtener_usuarios(self) -> List[str]:
        try:
            with self.get_db_connection() as conn:
                cursor = conn.execute("SELECT chat_id FROM users")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error leyendo usuarios: {e}")
            return []

    def leer_ultimo_update_id(self) -> Optional[int]:
        try:
            with open(LAST_UPDATE_PATH, "r") as f:
                return int(f.read().strip()) if f.read().strip() else None
        except (FileNotFoundError, ValueError):
            return None
        except Exception as e:
            logger.error(f"Error leyendo update_id: {e}")
            return None

    def guardar_ultimo_update_id(self, update_id: int):
        try:
            Path(LAST_UPDATE_PATH).parent.mkdir(exist_ok=True)
            with open(LAST_UPDATE_PATH, "w") as f:
                f.write(str(update_id))
        except Exception as e:
            logger.error(f"Error guardando update_id: {e}")

    def obtener_clima(self) -> Optional[Tuple[float, float]]:
        """Retorna (temp, humedad) o None"""
        if not CIUDAD or not API_KEY:
            logger.error("Configuración clima incompleta")
            return None

        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": CIUDAD,
            "appid": API_KEY,
            "units": "metric",
            "lang": "es"
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data.get("cod") != 200:
                logger.error(f"API Clima error: {data}")
                return None

            main = data.get("main", {})
            temp = main.get("temp")
            humedad = main.get("humidity")
            
            if temp is None or humedad is None:
                logger.error("Datos clima incompletos")
                return None

            return float(temp), float(humedad)
            
        except requests.RequestException as e:
            logger.error(f"Error API Clima: {e}")
            return None

    def calcular_ith(self, temp: float, humedad_pct: float) -> float:
        """Fórmula THI mejorada para bovinos"""
        # Fórmula específica para bovinos (Temperatura-Humedad Index)
        thi = (1.8 * temp + 32) - (0.55 - 0.0055 * humedad_pct) * (1.8 * temp - 26)
        return thi

    def generar_mensaje(self, ith: float, temp: float, humedad_pct: float) -> Tuple[str, str]:
        if ith < 72:
            estado, emoji, mensaje = "verde", "🟢", "CONFORT (sin estrés térmico)"
        elif ith < 79:
            estado, emoji, mensaje = "amarillo", "🟡", "ALERTA - Revisar agua y sombra"
        elif ith < 84:
            estado, emoji, mensaje = "naranja", "🟠", "PELIGRO - Evitar movimientos"
        else:
            estado, emoji, mensaje = "rojo", "🔴", "EMERGENCIA - Riesgo alto"

        texto = f"""🌡️ *ITH: {ith:.1f}*
🌡️ *Temp:* {temp:.1f}°C
💧 *Humedad:* {humedad_pct:.0f}%
{emoji} *{mensaje}*"""
        return estado, texto

    def enviar_mensaje(self, chat_id: str, mensaje: str):
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": mensaje,
            "parse_mode": "Markdown"  # Para negritas
        }
        try:
            response = requests.post(url, data=data, timeout=15)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error enviando a {chat_id}: {e}")

    def enviar_multimedia(self, chat_id: str, estado: str):
        """Multimedia con mejor manejo de errores"""
        multimedia_map = {
            "verde": ("images/verde.mp4", "sendVideo", "video"),
            "amarillo": ("images/amarillo.gif", "sendAnimation", "animation"),
            "naranja": ("images/naranja.jpg", "sendPhoto", "photo"),
            "rojo": ("images/rojo.png", "sendPhoto", "photo")
        }
        
        if estado not in multimedia_map:
            return
            
        path, method, field = multimedia_map[estado]
        if not os.path.exists(path):
            logger.warning(f"Multimedia no encontrada: {path}")
            return
            
        url = f"https://api.telegram.org/bot{TOKEN}/{method}"
        try:
            with open(path, "rb") as f:
                requests.post(url, 
                           data={"chat_id": chat_id}, 
                           files={field: f}, 
                           timeout=30)
        except Exception as e:
            logger.error(f"Error multimedia {estado}: {e}")

    def enviar_ith_a_todos(self):
        clima = self.obtener_clima()
        if not clima:
            logger.error("No se pudo obtener clima")
            return

        temp, humedad = clima
        ith = self.calcular_ith(temp, humedad)
        estado, mensaje = self.generar_mensaje(ith, temp, humedad)

        usuarios = self.obtener_usuarios()
        if not usuarios:
            logger.info("No hay usuarios suscritos")
            return

        logger.info(f"Enviando ITH {ith:.1f} a {len(usuarios)} usuarios")
        
        for chat_id in usuarios:
            self.enviar_multimedia(chat_id, estado)
            self.enviar_mensaje(chat_id, mensaje)

    def procesar_update(self, update):
        try:
            if "message" not in update:
                return

            chat_id = str(update["message"]["chat"]["id"])
            texto = update["message"].get("text", "").strip()

            if texto == "/start":
                self.guardar_usuario(chat_id)
                self.enviar_bienvenida(chat_id)

            elif texto == "/estado":
                self.enviar_ith_usuario(chat_id)

            elif texto == "/usuarios":
                if chat_id == 1562651623:  # Agregar tu chat_id
                    count = len(self.obtener_usuarios())
                    self.enviar_mensaje(chat_id, f"👥 Usuarios suscritos: {count}")

        except Exception as e:
            logger.error(f"Error procesando update: {e}")

    def enviar_bienvenida(self, chat_id: str):
        try:
            with open("images/presentacion.png", "rb") as f:
                url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
                requests.post(url, data={"chat_id": chat_id}, files={"photo": f})
        except:
            pass  # No falla si no hay imagen

        mensaje = """✅ *¡Te suscribiste a alertas ITH!*

⏰ *Horarios:* 10:00, 12:00, 16:00
📱 *Comandos:*
/estado - ITH actual

*Sape loquitaaaaaaaaaa 🐄💨*"""
        self.enviar_mensaje(chat_id, mensaje)

    def enviar_ith_usuario(self, chat_id: str):
        clima = self.obtener_clima()
        if not clima:
            self.enviar_mensaje(chat_id, "❌ Error obteniendo clima")
            return

        temp, humedad = clima
        ith = self.calcular_ith(temp, humedad)
        estado, mensaje = self.generar_mensaje(ith, temp, humedad)

        self.enviar_multimedia(chat_id, estado)
        self.enviar_mensaje(chat_id, mensaje)

    def escuchar_actualizaciones(self):
        """Polling mejorado"""
        offset = self.last_update_id + 1 if self.last_update_id else None
        
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        params = {
            "timeout": 60,
            "allowed_updates": ["message"]
        }
        if offset:
            params["offset"] = offset

        try:
            response = requests.get(url, params=params, timeout=70).json()
            
            if "result" not in response:
                return

            for update in response["result"]:
                self.last_update_id = update["update_id"]
                self.guardar_ultimo_update_id(self.last_update_id)
                self.procesar_update(update)

        except Exception as e:
            logger.error(f"Error getUpdates: {e}")

    def signal_handler(self, signum, frame):
        logger.info("Recibida señal de terminación")
        self.cleanup()
        exit(0)

    def cleanup(self):
        logger.info("Limpiando recursos...")

    def run(self):
        logger.info(f"🚀 Bot iniciado - CIUDAD: {CIUDAD}")
        while True:
            try:
                ahora = datetime.now().strftime("%H:%M")
                self.escuchar_actualizaciones()

                # Envío programado
                if ahora in HORAS and self.ultima_ejecucion != ahora:
                    self.enviar_ith_a_todos()
                    self.ultima_ejecucion = ahora
                    logger.info(f"ITH enviado a las {ahora}")

                time.sleep(15)  # Más eficiente

            except KeyboardInterrupt:
                logger.info("Deteniendo bot...")
                break
            except Exception as e:
                logger.error(f"Error en loop principal: {e}")
                time.sleep(30)

if __name__ == "__main__":
    bot = ITHBot()
    bot.run()