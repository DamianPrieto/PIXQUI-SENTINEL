# Este es un documento que recopila todas las funciones y clases
# definidas en los archivos del proyecto para facilitar su referencia.

#Librerías importadas   

from pathlib import Path
import pandas as pd
import numpy as np  
import duckdb

# Funcion para extraer y combinar datos de archivos según el año

def extract_arch(año):
    # ... (Toda tu lógica de rutas y nombres de archivo se queda IGUAL) ...
    ruta_base = Path(__file__).resolve().parent.parent / 'data'
    ruta_carpeta = ruta_base / f'ssa_egresos_{año}'
    
    # ... (Definición de columnas y nombres de archivo IGUAL) ...
    col_egre = ['ID', 'CLUES', 'EGRESO', 'INGRE', 'DIAS_ESTA', 'EDAD', 'SEXO', 
                'PESO', 'TALLA', 'ENTIDAD', 'MUNIC', 'LOC', 'SERVICIOINGRE', 
                'SERVICIO02', 'SERVICIO03', 'SERVICIOEGRE', 'PROCED', 'CLUESPROCED',
                'MOTEGRE', 'CLUESREFERIDO', 'DIAG_INI', 'AFECPRIN',
                'VEZ', 'CAUSAEXT']
    col_afec = ['ID', 'AFEC', 'NUMAFEC']
    col_proc = ['ID', 'PROMED', 'TIPO']

# Definimos nombres de archivos
    nom_egre = 'Egresos.txt' if año > 2018 else f'EGRESO_{año}.csv'
    nom_afec = 'Afecciones.txt' if año > 2018 else f'AFECCIONES_{año}.csv'
    nom_proc = 'Procedimientos.txt' if año > 2018 else f'PROCEDIMIENTOS_{año}.csv'
    
    # Definimos separador
    if año == 2017: sep = '|'
    elif año >= 2020: sep = '|'
    else: sep = ','

    # --- AQUÍ CAMBIAMOS LA FUNCIÓN DE CARGA ---
    def cargar_flexible(archivo, columnas_deseadas):
        path = ruta_carpeta / archivo
        if not path.exists():
            print(f"Archivo no encontrado: {archivo}")
            return pd.DataFrame(columns=columnas_deseadas)
        
        # Leemos headers para no fallar
        try:
            sample = pd.read_csv(path, sep=sep, nrows=0, encoding='latin-1')
            cols_en_archivo = sample.columns.tolist()
        except:
            return pd.DataFrame(columns=columnas_deseadas)

        # Filtramos solo las que existen
        cols_a_cargar = [c for c in columnas_deseadas if c in cols_en_archivo]
        
        # Cargamos el DF
        try:
            df = pd.read_csv(path, sep=sep, usecols=cols_a_cargar, encoding='utf-8', low_memory=False)
        except:
            df = pd.read_csv(path, sep=sep if año != 2017 else "|", usecols=cols_a_cargar, encoding='latin-1', low_memory=False)

        # Limpieza CRÍTICA del ID (Para que SQL pueda unir después)
        if 'ID' in df.columns:
            # Quitamos decimales (.0) y convertimos a número limpio
            df['ID'] = pd.to_numeric(df['ID'], errors='coerce').fillna(0).astype('int64')
        
        # Agregamos el AÑO para que no se pierda el rastro
        df['ANIO'] = año
        
        return df

    # Cargamos las 3 partes
    print(f"  Extrayendo {año}...")
    df_egre = cargar_flexible(nom_egre, col_egre)
    df_afec = cargar_flexible(nom_afec, col_afec)
    df_proc = cargar_flexible(nom_proc, col_proc)

    # --- EL GRAN CAMBIO: Retornamos las 3 piezas SEPARADAS ---
    # No hacemos merge aquí. Dejamos que DuckDB lo haga después.
    return df_egre, df_afec, df_proc

# Funcion de estandarizacion de columnas 

def estand_geo(series, ceros):
    # Usamos .fillna(0) para que no truene si hay vacíos
    # Si es nulo, pone 0, luego convierte a entero, luego a string y rellena ceros
    return series.fillna(0).astype(int).astype(str).str.zfill(ceros)

def limpiar_outliers(series, min_val, max_val):

    # Forzamos a numérico (convierte errores a NaN)
    series = pd.to_numeric(series, errors='coerce')
    
    # Convertir el código de error 999 (y 998) a NaN explícitamente
    col = series.replace([999, 999.0, 998, 998.0], np.nan)
    
    # Validar rangos biológicos (ej. nadie pesa 0 kg ni 1000 kg)
    # Usamos np.where: "Si es válido déjalo, si no, pon NaN"
    # Condición: Que sea mayor a min_val Y menor a max_val
    col = np.where((col >= min_val) & (col <= max_val), col, np.nan)
    
    return col

def fechas_inteligentes(series):

    # Convertimos a texto y partimos donde haya un espacio. Nos quedamos con la primera parte.
    # Esto elimina el " 00:00:00" automáticamente.
    s = series.astype(str).str.split(' ').str[0].str.strip()
    
    # Creamos una Serie vacía para guardar los resultados
    resultado = pd.Series(pd.NaT, index=series.index)
    
    # CASO A: TIENEN GUION (-) -> Asumimos YYYY-MM-DD
    mask_guion = s.str.contains('-', na=False)
    resultado[mask_guion] = pd.to_datetime(s[mask_guion], format='%Y-%m-%d', errors='coerce')
    
    # CASO B: TIENEN DIAGONAL (/) -> Asumimos DD/MM/YYYY
    mask_diagonal = s.str.contains('/', na=False)
    resultado[mask_diagonal] = pd.to_datetime(s[mask_diagonal], dayfirst=True, errors='coerce')
    
    return resultado

# Funcion para crear la base de datos completa PIXQUI-SENTINEL

# 3. BASE DE DATOS (Esquema completo y corregido)
def inicializar_db(db_path="data/pixqui_sentinel.duckdb"):
    con = duckdb.connect(db_path)
    
    # Borramos tablas si existen para reiniciar limpio (Opcional)
    # con.execute("DROP TABLE IF EXISTS EGRESOS_BASE; DROP TABLE IF EXISTS AFECCIONES; DROP TABLE IF EXISTS PROCEDIMIENTOS;")

    print("Construyendo esquema en DuckDB...")
    
    # TABLA 1: EGRESOS_BASE (Todas las columnas explicítas)
    con.execute("""
    CREATE TABLE IF NOT EXISTS EGRESOS_BASE (
        ID BIGINT,
        ANIO SMALLINT,
        CLUES VARCHAR,
        EGRESO DATE,
        INGRE DATE,
        DIAS_ESTA SMALLINT,
        EDAD SMALLINT,
        SEXO TINYINT,
        PESO DECIMAL(5,2),
        TALLA DECIMAL(5,2),
        IMC DECIMAL(5,2),
        CVE_GEO VARCHAR,
        ENTIDAD CHAR(2),
        MUNIC CHAR(3),
        LOC CHAR(4),
        SERVICIOINGRE VARCHAR,
        SERVICIO02 VARCHAR,
        SERVICIO03 VARCHAR,
        SERVICIOEGRE VARCHAR,
        PROCED VARCHAR,        -- Procedimiento principal
        CLUESPROCED VARCHAR,
        MOTEGRE TINYINT,
        CLUESREFERIDO VARCHAR,
        DIAG_INI VARCHAR(6),
        AFECPRIN VARCHAR(6),
        VEZ TINYINT,
        CAUSAEXT VARCHAR
    );
    """)

    # TABLA 2: AFECCIONES
    con.execute("""
    CREATE TABLE IF NOT EXISTS AFECCIONES (
        ID BIGINT,
        AFEC VARCHAR(6),
        NUMAFEC SMALLINT,
        ANIO SMALLINT
    );
    """)

    # TABLA 3: PROCEDIMIENTOS
    con.execute("""
    CREATE TABLE IF NOT EXISTS PROCEDIMIENTOS (
        ID BIGINT,
        PROMED VARCHAR(6),
        TIPO CHAR(1),
        ANIO SMALLINT
    );
    """)
    
    con.close()
    print(f"Bóveda DuckDB lista en: {db_path}")
    return db_path