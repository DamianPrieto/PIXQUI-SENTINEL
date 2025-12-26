import pandas as pd
from pathlib import Path
from src.etl.base import CargadorBase

class CargadorConapo(CargadorBase):
    """
    Especialista en cargar datos de Población.
    Hereda de CargadorBase.
    """

    def extraer(self):
        # Definimos la ruta del archivo CONAPO
        ruta_archivo = Path(__file__).resolve().parent.parent.parent / "data" / "CONAPO_POB.csv"
        
        print(f"Leyendo archivo CONAPO: {ruta_archivo.name}")
        
        try:
            # Tip: encoding='latin-1' es común en gobierno de México
            self.df = pd.read_csv(ruta_archivo, encoding='latin-1')
        except FileNotFoundError:
            print("Error: No se encuentra el archivo de CONAPO.")
            self.df = pd.DataFrame()

    def transformar(self):
        print(f"🧹 [CONAPO] Transformando datos para el año {self.anio}...")
        
        # 1. Validación de columnas críticas ANTES de procesar
        # Es buena práctica verificar que 'ANIO' exista para no filtrar a ciegas
        if 'ANIO' not in self.df.columns:
            print("Error: La columna 'ANIO' no existe en el CSV.")
            self.df = pd.DataFrame() # Vaciamos y salimos
            return

        # 2. FILTRADO POR AÑO (El paso que faltaba)
        # Solo nos quedamos con los datos del año que pidió el Main
        self.df = self.df[self.df['ANIO'] == int(self.anio)]
        
        if self.df.empty:
            print(f"No hay datos de CONAPO para el año {self.anio}.")
            return

        # 3. Construcción de CVE_GEO
        # Optimizacion: Hacemos esto DESPUES de filtrar el año para procesar menos filas
        self.df['CVE_GEO'] = self.df['CLAVE'].astype(str).str.zfill(5)

        # 4. Selección de Columnas
        columnas_relevantes = [
            'SEXO', 'ANIO', 'POB_00_011', 'POB_012_29', 
            'POB_30_59', 'POB_60_mm', 'POB_TOTAL', 'CVE_GEO'
        ]
        
        # Verificamos que todas las columnas existan para evitar KeyError
        # Esto usa "List Comprehension" para intersectar las columnas que pediste con las que existen
        cols_existentes = [c for c in columnas_relevantes if c in self.df.columns]
        
        self.df = self.df[cols_existentes]

