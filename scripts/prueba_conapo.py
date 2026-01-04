from src.database.connection import db
from src.etl.loaders import CargadorConapo
import time

def carga_masiva_poblacion():
    print("INICIANDO CARGA DE POBLACIÓN (1990 - 2040)")
    
    # 1. Limpieza Inicial (Idempotencia)
    # Como vamos a cargar todo el historial, es más seguro borrar la tabla y crearla de cero
    # para asegurar que no tengas duplicados si corres el script dos veces.
    print("Reiniciando tabla DIM_POBLACION...")
    db.ejecutar_query("DROP TABLE IF EXISTS DIM_POBLACION")
    
    sql_create = """
    CREATE TABLE DIM_POBLACION (
        CVE_GEO VARCHAR, 
        ANIO INTEGER, 
        SEXO VARCHAR,
        POB_00_011 INTEGER, 
        POB_012_29 INTEGER,
        POB_30_59 INTEGER, 
        POB_60_mm INTEGER, 
        POB_TOTAL INTEGER,
        NOM_MUN VARCHAR
    );
    """
    db.ejecutar_query(sql_create)
    
    # Creamos el índice de una vez para que las inserciones se organicen
    db.ejecutar_query("CREATE INDEX idx_poblacion_geo_anio ON DIM_POBLACION (CVE_GEO, ANIO)")

    # 2. El Bucle del Tiempo
    inicio = time.time()
    total_filas = 0
    
    # range(1990, 2041) llega hasta 2040
    for anio in range(1990, 2041):
        try:
            loader = CargadorConapo(anio)
            df = loader.procesar()
            
            if not df.empty:
                # Usamos tu método optimizado
                db.cargar_df("DIM_POBLACION", df)
                total_filas += len(df)
            else:
                print(f"Salto año {anio}: Sin datos.")
                
        except Exception as e:
            print(f"Error crítico en {anio}: {e}")

    fin = time.time()
    mins = (fin - inicio) / 60
    
    print("-" * 50)
    print(f"CARGA FINALIZADA.")
    print(f"Años procesados: 1990-2040")
    print(f"Total registros inyectados: {total_filas:,}")
    print(f"Tiempo total: {mins:.2f} minutos")
    
    # 3. Verificación rápida
    count = db.ejecutar_query("SELECT COUNT(*) FROM DIM_POBLACION").fetchone()[0]
    print(f"Verificación en DB: {count} filas existentes.")
    
    db.cerrar()

if __name__ == "__main__":
    carga_masiva_poblacion()