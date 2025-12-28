from src.database.connection import db # Importamos la instancia lista

class EpidemiologyService:
    def __init__(self):
        self.db = db # Usamos el Singleton directamente

    def obtener_tasa_multiple(self, anio, cve_geo, lista_enfermedades=None):
        filtro_cie = ""
        if lista_enfermedades:
            codigos_str = "', '".join(lista_enfermedades)
            # AQUI ESTÁ LA MAGIA: Usamos LEFT(AFECPRIN, 3)
            filtro_cie = f"AND LEFT(AFECPRIN, 3) IN ('{codigos_str}')"

        sql = f"""
        WITH Poblacion AS (
            SELECT SUM(POB_TOTAL) as habitantes 
            FROM DIM_POBLACION 
            WHERE ANIO = {anio} AND CVE_GEO = '{cve_geo}'
        )
        SELECT 
            LEFT(e.AFECPRIN, 3) as CATEGORIA, -- Agrupamos por los primeros 3 caracteres
            COUNT(*) as casos,
            p.habitantes,
            (CAST(COUNT(*) AS FLOAT) / p.habitantes) * 100000 as tasa
        FROM EGRESOS_BASE e, Poblacion p
        WHERE 
            e.ANIO = {anio} 
            AND e.CVE_GEO = '{cve_geo}'
            {filtro_cie}
        GROUP BY LEFT(e.AFECPRIN, 3), p.habitantes -- Agrupamos también aquí
        ORDER BY tasa DESC;
        """
        return self.db.ejecutar_query(sql).df()
    
    def obtener_evolucion_estado(self, cve_estado, letra_cie, anio_inicio, anio_fin):
        """
        Obtiene la serie de tiempo para TODOS los municipios de un estado.
        cve_estado: '07' (Chiapas)
        letra_cie: 'I' (Cardiovasculares)
        """
        print(f"📊 Generando matriz de calor para estado {cve_estado}, bloque {letra_cie}...")

        sql = f"""
        WITH Poblacion AS (
            SELECT ANIO, CVE_GEO, SUM(POB_TOTAL) as habitantes
            FROM DIM_POBLACION
            WHERE 
                LEFT(CVE_GEO, 2) = '{cve_estado}' 
                AND ANIO BETWEEN {anio_inicio} AND {anio_fin}
            GROUP BY ANIO, CVE_GEO
        ),
        Casos AS (
            SELECT ANIO, CVE_GEO, COUNT(*) as enfermos
            FROM EGRESOS_BASE
            WHERE 
                LEFT(CVE_GEO, 2) = '{cve_estado}' -- Filtramos por estado
                AND LEFT(AFECPRIN, 1) = '{letra_cie}' -- Filtramos bloque 'I'
                AND ANIO BETWEEN {anio_inicio} AND {anio_fin}
            GROUP BY ANIO, CVE_GEO
        )
        SELECT 
            p.ANIO,
            p.CVE_GEO,
            -- Agregamos el nombre del municipio para que se vea bonito (si tienes catalogo)
            -- Por ahora usamos la clave
            c.enfermos,
            p.habitantes,
            (CAST(COALESCE(c.enfermos, 0) AS FLOAT) / p.habitantes) * 100000 as tasa
        FROM Poblacion p
        LEFT JOIN Casos c 
            ON p.ANIO = c.ANIO AND p.CVE_GEO = c.CVE_GEO
        ORDER BY p.CVE_GEO, p.ANIO;
        """
        return self.db.ejecutar_query(sql).df()
    

servicio = EpidemiologyService()

# ¡Un solo viaje a la base de datos! ⚡
mis_enfermedades = ['E11', 'E12', 'E13', 'E14']
df = servicio.obtener_tasa_multiple(2022, '01001', mis_enfermedades)

print(df)