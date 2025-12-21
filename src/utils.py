# Este es un documento que recopila todas las funciones y clases
# definidas en los archivos del proyecto para facilitar su referencia.

#Librerías importadas   

from pathlib import Path
import pandas as pd
import numpy as np  

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
