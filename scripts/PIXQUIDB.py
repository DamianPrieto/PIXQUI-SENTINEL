from src.utils import extract_arch, inicializar_db, estand_geo, limpiar_outliers, fechas_inteligentes, sanitizar_numericos
import duckdb
import pandas as pd
import numpy as np

if __name__ == "__main__":

    print(" Iniciando carga masiva a la base de datos PIXQUI-SENTINEL...")
    print("-" * 50)
    # 1. Preparar la Base de Datos
    DB_PATH = inicializar_db()
    con = duckdb.connect(DB_PATH)

    # 2. Bucle por años (Ejemplo 2022 y 2023)
    anios_a_procesar = np.arange(2010, 2026) # Desde 2010 hasta 2025

    for anio in anios_a_procesar:
        print(f"\nPROCESANDO AÑO {anio}...")
    
        # A. Extracción (Obtenemos 3 tablas separadas)
        df_egre, df_afec, df_proc = extract_arch(anio)
    
        # B. Limpieza Rápida (Solo en df_egre que es la importante)
        if not df_egre.empty:
            print("Limpiando fechas y geografía y outlers de peso y talla...")
            df_egre['INGRE'] = fechas_inteligentes(df_egre['INGRE'])
            df_egre['EGRESO'] = fechas_inteligentes(df_egre['EGRESO'])
            df_egre['ENTIDAD'] = estand_geo(df_egre['ENTIDAD'], 2)
            df_egre['MUNIC'] = estand_geo(df_egre['MUNIC'], 3)
            df_egre['LOC'] = estand_geo(df_egre['LOC'], 4)
            df_egre['PESO'] = limpiar_outliers(df_egre['PESO'], 0, 700)
            df_egre['TALLA'] = limpiar_outliers(df_egre['TALLA'], 0, 250)
            df_egre['EDAD'] = limpiar_outliers(df_egre['EDAD'], 0, 120)
            df_egre['SEXO'] = limpiar_outliers(df_egre['SEXO'], 0, 9)

            # Limpieza de columnas numéricas

            # 1. DEFINIMOS QUÉ COLUMNAS NO PUEDEN TENER TEXTO BAJO NINGUNA CIRCUNSTANCIA
            # DEFINIMOS LAS REGLAS: { Columna : Valor Máximo Permitido }
            reglas_limpieza = {
                # --- SERVICIOS (Límite 500 según tu indicación) ---
                'SERVICIOINGRE': 500,
                'SERVICIOEGRE': 500,
                'SERVICIO02': 500,
                'SERVICIO03': 500,

                # --- DATOS DEL PACIENTE ---
                'MOTEGRE': 9,       # Los códigos oficiales suelen ser del 1 al 7 (9 es No especificado)
                'VEZ': 99,          # Es improbable que alguien vaya más de 99 veces por lo mismo
                'EDAD': 120,        # Límite biológico humano (filtra errores como 999 o años locos)
                'DIAS_ESTA': 3000,  # Un margen amplio (aprox 8 años) por si hay estancias largas reales
                'NUMAFEC': 50       # Rara vez se registran más de 50 afecciones secundarias
            }
        
            # 2. APLICAMOS LA SANITIZACIÓN
            # Adiós "KLAJDALKSJ", hola NaN.
            df_egre = sanitizar_numericos(df_egre, reglas_limpieza)

            print("Calculando variables derivadas (IMC Y CVE_GEO)...")

            # FUSIONAMOS PARA CREAR UNA LLAVE DE IDENTIFICACION
            df_egre['CVE_GEO'] = df_egre['ENTIDAD'] + df_egre['MUNIC']

            # CALCULAMOS IMC (Índice de Masa Corporal)
            df_egre['IMC'] = (df_egre['PESO'] / ((df_egre['TALLA'] / 100) ** 2)).round(2)

            # Hacemos un outler para el IMC (Valores entre 10 y 60 son razonables)
            df_egre['IMC'] = np.where((df_egre['IMC'] >= 10) & (df_egre['IMC'] <= 60), df_egre['IMC'], np.nan)

            print("Datos limpios y listos para inyección.")
        
            # C. Inyección (El momento mágico)
            print("Inyectando a DuckDB...")
        
            # Insertamos Egresos (Usando by name para que coincidan las columnas automáticamente)
            con.sql("INSERT INTO EGRESOS_BASE BY NAME SELECT * FROM df_egre")
        
            # Insertamos Afecciones (Si hay datos)
            if not df_afec.empty:
                con.sql("INSERT INTO AFECCIONES BY NAME SELECT * FROM df_afec")
            
            # Insertamos Procedimientos (Si hay datos)
            if not df_proc.empty:
                con.sql("INSERT INTO PROCEDIMIENTOS BY NAME SELECT * FROM df_proc")
            print(f"Año {anio} cargado exitosamente.")
        else:
            print(f"No se encontraron datos para {anio}")

    print("\nCARGA TERMINADA.")
    con.close()