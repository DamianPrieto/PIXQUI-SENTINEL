from src.database.connection import db

def prueba_cruce_datos():
    print("VALIDANDO CRUCE: Egresos vs Población")
    
    # 1. Definimos una consulta de Alto Nivel
    # Objetivo: Calcular la tasa de hospitalización en Aguascalientes (01001) en 2023
    # Fórmula: (Total Egresos / Población Total) * 100,000 habitantes
    
    sql = """
    -- PASO 1: Contar los enfermos (Agrupamos primero)
    WITH Casos AS (
        SELECT 
            ANIO, 
            CVE_GEO, 
            COUNT(*) as total_enfermos
        FROM EGRESOS_BASE 
        WHERE CVE_GEO = '01001' AND ANIO = 2022
        GROUP BY ANIO, CVE_GEO
    ),
    -- PASO 2: Calcular la población (Agrupamos primero)
    Poblacion AS (
        SELECT 
            ANIO, 
            CVE_GEO, 
            SUM(POB_TOTAL) as total_habitantes
        FROM DIM_POBLACION
        WHERE CVE_GEO = '01001' AND ANIO = 2022
        GROUP BY ANIO, CVE_GEO
    )
    -- PASO 3: Unir los resúmenes (Join 1 a 1)
    SELECT 
        c.ANIO,
        c.CVE_GEO,
        'Aguascalientes' as MUNICIPIO,
        c.total_enfermos,
        p.total_habitantes,
        (CAST(c.total_enfermos AS FLOAT) / p.total_habitantes) * 100000 as TASA_REAL
    FROM Casos c
    INNER JOIN Poblacion p 
        ON c.ANIO = p.ANIO AND c.CVE_GEO = p.CVE_GEO;
    """
    
    print("Ejecutando JOIN dimensional...")
    try:
        df_resultado = db.ejecutar_query(sql).df()
        
        if not df_resultado.empty:
            print("\n¡CRUCE EXITOSO!")
            print(df_resultado.to_string(index=False))
            print("\nInterpretación: Por cada 100,000 habitantes, X fueron hospitalizados.")
        else:
            print("\nEl query corrió, pero no trajo datos.")
            print("Posibles causas: No hay datos de egresos 2022 cargados o las CVE_GEO no coinciden.")
            
    except Exception as e:
        print(f"\nError en el JOIN: {e}")

    db.cerrar()

if __name__ == "__main__":
    prueba_cruce_datos()