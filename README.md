
**Sistema de Vigilancia Epidemiológica y Modelado Predictivo**
**Autor:** Angel Damian Prieto López | UPIBI - IPN

# PIXQUI: Vigilante Epidemiológico Multimodal

**PIXQUI** (del náhuatl *Tlapixqui*: Guardián/Centinela) es un ecosistema de inteligencia de datos diseñado para la vigilancia, estandarización y modelado predictivo de enfermedades de alto impacto en el territorio mexicano.

Inspirado en el concepto del **Yaotlapixqui** (Vigilante de Guerra), este sistema actúa como una defensa activa en la salud pública, transformando datos hospitalarios masivos en evidencia técnica para el combate contra las enfermedades no transmisibles.

---

## Objetivo del Proyecto
Desarrollar una infraestructura de datos robusta que integre y analice egresos hospitalarios a nivel nacional (periodo 2010-2025) con el fin de:
* **Analizar Tendencias:** Identificar patrones de morbi-mortalidad cardiovascular y sistémica.
* **Estandarizar Big Data:** Unificar registros históricos de diversas administraciones en un esquema SQL relacional.
* **Georreferenciación:** Detectar clústeres de riesgo y focos rojos epidemiológicos.
* **Modelado Predictivo:** Evaluar factores de riesgo (como el IMC) para anticipar cargas hospitalarias.

---

## Stack Tecnológico
* **Lenguaje:** Python 3.12+ (Procesamiento y limpieza de datos).
* **Motor de Base de Datos:** SQLite3 (Arquitectura relacional ligera y eficiente).
* **Control de Versiones:** Git & GitHub (Gestión profesional de cambios).
* **Documentación:** Markdown (.md) bajo estándares de ingeniería de software.
* **Librerías:** Pandas, NumPy, SymPy (Cálculo simbólico y matemático).

---

## Estructura del Repositorio
```text
PROYECTO_PIXQUI/
├── data/           # Repositorio local de datasets y DB (Ignorado por Git)
├── docs/           # Diccionarios de variables y documentación técnica
├── notebooks/      # Análisis exploratorio (EDA) y experimentación
├── scripts/        # Motores de carga y scripts de automatización
├── src/            # Código fuente, utilidades y funciones de limpieza
└── README.md       # Documentación principal del sistema