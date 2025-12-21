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
    
    col_afec = ['ID', 'AFEC']

    col_proc = ['ID', 'PROMED', 'TIPO']

    if not ruta_carpeta.exists():
        raise FileNotFoundError(f"La carpeta para el año {año} no existe.")

    # Lógica de carga según el periodo histórico
    if año > 2018:
        # Formato Moderno: TXT con separador de tubería |
        df_egre = pd.read_table(ruta_carpeta / 'Egresos.txt', sep='|', usecols=col_egre, low_memory=False)
        df_afec = pd.read_table(ruta_carpeta / 'Afecciones.txt', sep='|', usecols=col_afec, low_memory=False)
        df_proc = pd.read_table(ruta_carpeta / 'Procedimientos.txt', sep='|', usecols=col_proc, low_memory=False)
    else:
        # Formato Clásico: CSV con comas
        df_egre = pd.read_csv(ruta_carpeta / f'EGRESO_{año}.csv', usecols=col_egre, low_memory=False)
        df_afec = pd.read_csv(ruta_carpeta / f'AFECCIONES_{año}.csv', usecols=col_afec, low_memory=False)
        df_proc = pd.read_csv(ruta_carpeta / f'PROCEDIMIENTOS_{año}.csv', usecols=col_proc, low_memory=False)

    # El Triple Merge Encadenado
    # Unimos Afecciones con Egresos, y el resultado con Procedimientos
    df_combinado = df_afec.merge(df_egre, on='ID', how='inner').merge(df_proc, on='ID', how='inner')


    return df_combinado

# Funcion de estandarizacion de columnas 

def estand_geo(series, ceros):
    # Usamos .fillna(0) para que no truene si hay vacíos
    # Si es nulo, pone 0, luego convierte a entero, luego a string y rellena ceros
    return series.fillna(0).astype(int).astype(str).str.zfill(ceros)

def limpiar_outliers(series, min_val, max_val):
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



def inicializar_db(db_path="data/pixqui_sentinel.duckdb"):
    """
    Crea la base de datos y la tabla con tipos de datos optimizados 
    para el análisis nacional de enfermedades cardiovasculares.
    """
    # 1. Conexión (se crea el archivo si no existe)
    con = duckdb.connect(db_path)
    
    # 2. Creación de la tabla con tipos de datos de alto rendimiento
    con.execute("""
    CREATE TABLE IF NOT EXISTS egresos_ecv (
        -- Identificadores masivos
        id_registro BIGINT PRIMARY KEY,
        
        -- Geografía (Cadenas de longitud fija para velocidad)
        entidad CHAR(2),
        municipio CHAR(3),
        localidad CHAR(4),
        clave_geo CHAR(9), 
        clues VARCHAR(15),
        
        -- Datos Temporales (Tipo DATE nativo)
        fecha_ingreso DATE,
        fecha_egreso DATE,
        dias_estancia SMALLINT,
        
        -- Perfil Biomédico (Optimización de espacio)
        edad SMALLINT, 
        sexo TINYINT, -- 1:H, 2:M
        peso DECIMAL(5,2), 
        talla DECIMAL(5,2),
        imc DECIMAL(5,2),
        
        -- Diagnósticos y Resultados
        afeccion VARCHAR(5),
        diagnostico_inicio VARCHAR(5),
        motivo_egreso TINYINT, -- 6 = Defunción
        
        -- Otros indicadores
        vez TINYINT,
        causa_ext VARCHAR(5),
        promed SMALLINT,
        tipo CHAR(1)
    );
    """)
    
    con.close()
    print(f"🛡️ Bóveda DuckDB inicializada en: {db_path}")
    return db_path