# Este es un documento que recopila todas las funciones y clases
# definidas en los archivos del proyecto para facilitar su referencia.

#Librerías importadas   

from pathlib import Path
import pandas as pd
import numpy as np  
import duckdb

# Funcion para extraer y combinar datos de archivos según el año

def extract_arch(año):

    # Ajustamos la ruta si utils.py está en la carpeta 'src'

    ruta_base = Path(__file__).resolve().parent.parent / 'data'

    ruta_carpeta = ruta_base / f'ssa_egresos_{año}'

    #Ajustamos columnas deseadas para optimizar memoria

    col_egre = ['ID', 'CLUES', 'EGRESO', 'INGRE', 'DIAS_ESTA', 'EDAD', 'SEXO',
                'PESO', 'TALLA', 'ENTIDAD', 'MUNIC', 'LOC', 'SERVICIOINGRE',
                'SERVICIO02', 'SERVICIO03', 'SERVICIOEGRE', 'PROCED', 'CLUESPROCED',
                'MOTEGRE', 'CLUESREFERIDO', 'DIAG_INI', 'AFECPRIN',
                'VEZ', 'CAUSAEXT']

    col_afec = ['ID', 'AFEC', 'NUMAFEC']
    col_proc = ['ID', 'PROMED', 'TIPO']

    # Definimos qué archivos buscar según el año
    nom_egre = 'Egresos.txt' if año > 2018 else f'EGRESO_{año}.csv'
    nom_afec = 'Afecciones.txt' if año > 2018 else f'AFECCIONES_{año}.csv'
    nom_proc = 'Procedimientos.txt' if año > 2018 else f'PROCEDIMIENTOS_{año}.csv'
   
    if año == 2017:
        sep = '|'
    elif año >= 2020:
        sep = '|'
    elif año >= 2018:
        sep = ','
    else:
        sep = ','
   
    def cargar_flexible(archivo, columnas_deseadas):
        path = ruta_carpeta / archivo
        if not path.exists():
            print(f"{archivo} no encontrado. Generando datos vacíos.")
            return pd.DataFrame(columns=['ID'])
        # Leemos solo las columnas que REALMENTE existen en el archivo
        # para evitar el error de 'Usecols do not match'
        cols_en_archivo = pd.read_csv(path, sep=sep, nrows=0).columns.tolist()
        cols_a_cargar = [c for c in columnas_deseadas if c in cols_en_archivo]
        try:
            # Intento normal
            df = pd.read_csv(path, sep=sep, usecols=cols_a_cargar, encoding='utf-8', low_memory=False)

        except UnicodeDecodeError:
            # Si falla el encoding (caso 2017, con separacion por |)
            df = pd.read_csv(path, sep= "|", usecols=cols_a_cargar, encoding='latin-1', low_memory=False)

        except KeyError:
            # El caso de 2019 con separacion por ,
            pd.read_csv(path, sep= ",", usecols=cols_a_cargar, encoding='latin-1', low_memory=False)
       
        # --- EL TRUCO MAESTRO ---
        # Forzamos ID a string para evitar el error de merge

        if 'ID' in df.columns:
            df['ID'] = df['ID'].astype(str).str.strip().str.replace('.0', '', regex=False)
        return df

    # Carga de las 3 tablas con la nueva lógica flexible
    df_egre = cargar_flexible(nom_egre, col_egre)
    df_afec = cargar_flexible(nom_afec, col_afec)
    df_proc = cargar_flexible(nom_proc, col_proc)
    df_egre['ANIO'] = año
    df_afec['ANIO'] = año
    df_proc['ANIO'] = año

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
    con.execute("DROP TABLE IF EXISTS EGRESOS_BASE; DROP TABLE IF EXISTS AFECCIONES; DROP TABLE IF EXISTS PROCEDIMIENTOS;")

    print("Construyendo esquema en DuckDB...")
    
    # TABLA 1: EGRESOS_BASE (Todas las columnas explicítas)
    con.execute("""
    CREATE TABLE IF NOT EXISTS EGRESOS_BASE (
        ID VARCHAR,
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
        VEZ VARCHAR(10),
        CAUSAEXT VARCHAR
    );
    """)

    # TABLA 2: AFECCIONES
    con.execute("""
    CREATE TABLE IF NOT EXISTS AFECCIONES (
        ID VARCHAR,
        AFEC VARCHAR(6),
        NUMAFEC SMALLINT,
        ANIO SMALLINT
    );
    """)

    # TABLA 3: PROCEDIMIENTOS
    con.execute("""
    CREATE TABLE IF NOT EXISTS PROCEDIMIENTOS (
        ID VARCHAR,
        PROMED VARCHAR(6),
        TIPO CHAR(1),
        ANIO SMALLINT
    );
    """)
    
    con.close()
    print(f"Bóveda DuckDB lista en: {db_path}")
    return db_path

# Funcion para forzar a ser numeros

def sanitizar_numericos(df, reglas):
    """
    Recibe un diccionario: {'NOMBRE_COLUMNA': VALOR_MAXIMO}
    Ejemplo: {'MOTEGRE': 9, 'VEZ': 20}
    """
    for col, max_val in reglas.items():
        if col in df.columns:
            # 1. Fuerza bruta: Texto a Numero (letras -> NaN)
            series = pd.to_numeric(df[col], errors='coerce')
            
            # 2. Filtro de Rango: Si es < 0 o > max_val -> NaN
            # Usamos np.where porque es vectorizado (ultra rápido)
            df[col] = np.where((series >= 0) & (series <= max_val), series, np.nan)
            nulos = df[col].isna().sum()
            if nulos > 0:
                 print(f"Columna {col}: {nulos} datos inválidos convertidos a NULL")
            
    return df