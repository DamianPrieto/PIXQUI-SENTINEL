import duckdb
from pathlib import Path

class DatabaseManager:
    # Estas son variables DE CLASE (compartidas por todos)
    _instancia = None
    
    def __new__(cls, *args, **kwargs):
        """
        MAGIA NEGRA DE PYTHON (Patrón Singleton):
        Este método se ejecuta ANTES de __init__.
        Controla la creación del objeto. Si ya existe, devuelve el que ya estaba.
        """
        if cls._instancia is None:
            cls._instancia = super(DatabaseManager, cls).__new__(cls)
        return cls._instancia

    def __init__(self, db_filename="pixqui_sentinel.duckdb"):
        """
        Constructor estándar.
        Solo inicializamos variables si aún no están listas.
        """
        # Evitamos reiniciar si ya estaba creado
        if not hasattr(self, "inicializado"):
            # Define la ruta absoluta CORRECTA basada en la ubicación de este archivo
            base_dir = Path(__file__).resolve().parent.parent.parent # Sube desde src/database/ a la raiz
            self.ruta_db = base_dir / "data" / db_filename
            
            self.conexion = None
            self.inicializado = True
            print(f"⚙️ Configuración de DB cargada: {self.ruta_db}")

    def conectar(self):
        if self.conexion is None:
            print(f"🔌 Abriendo conexión a DuckDB...")
            # Convertimos Path a string porque DuckDB a veces es exigente
            self.conexion = duckdb.connect(str(self.ruta_db), config={'memory_limit': '8GB'})
        return self.conexion

    def ejecutar_query(self, query, read_only=False):
        con = self.conectar()
        # Aquí podriamos agregar try/except para capturar errores de SQL
        return con.execute(query)

    def cerrar(self):
        if self.conexion is not None:
            self.conexion.close()
            self.conexion = None
            print("🔌 Conexión cerrada y liberada.")

# --- INSTANCIA GLOBAL ---
# Creamos el objeto aquí mismo.
# Cualquiera que importe este archivo usará ESTA variable.
db = DatabaseManager()