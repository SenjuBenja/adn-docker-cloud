from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

# ------------------------------------------------------------
# Configuración de la API
# ------------------------------------------------------------
app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description=(
        "Compara archivos ADN descargados desde GitHub Releases, "
        "incluyendo archivos grandes en modo streaming."
    ),
    version="1.0.0",
)

# ------------------------------------------------------------
# CONSTANTES: TUS URLs REALES
# ------------------------------------------------------------

# 🔹 URLs de los archivos 'pequeños' (quarters o recortes)
URL_ADN_A_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
)
URL_ADN_B_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"
)

# 🔹 URLs de los archivos GRANDES (los ~1.2 GB que cortaste)
URL_ADN_A_GRANDE = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/"
    "GCA_000001405.29_GRCh38.p14--_genomic.fna"
)
URL_ADN_B_GRANDE = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/"
    "GCF_000001405.40_GRCh38.p14--_genomic.fna"
)

# 🔹 Líneas por batch para comparación grande (streaming)
BATCH_LINES = 10_000  # 10k líneas


# ------------------------------------------------------------
# Utilidad para archivos GRANDES (streaming por batches)
# ------------------------------------------------------------
def comparar_archivos_grandes(
    url_a: str,
    url_b: str,
    max_batches: Optional[int] = None,
) -> Path:
    """
    Descarga dos archivos grandes desde url_a y url_b y los compara línea por línea
    en batches de 10k líneas (streaming, sin cargar todo en RAM).

    Crea un archivo de texto con el reporte y devuelve la ruta a ese archivo.

    - max_batches = None  -> recorre TODO el archivo.
    - max_batches = N     -> procesa sólo N batches (N * 10k líneas).
    """

    resp_a = requests.get(url_a, stream=True)
    resp_b = requests.get(url_b, stream=True)

    if resp_a.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"No se pudo descargar A: {url_a} (status {resp_a.status_code})",
        )
    if resp_b.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"No se pudo descargar B: {url_b} (status {resp_b.status_code})",
        )

    # decode_unicode=True => vienen como str, sin el prefijo b'...'
    iter_a = resp_a.iter_lines(decode_unicode=True)
    iter_b = resp_b.iter_lines(decode_unicode=True)

    carpeta = Path("resultados")
    carpeta.mkdir(exist_ok=True)
    ruta_salida = carpeta / "reporte_grande.txt"

    line_number = 0
    batch_number = 0
    lines_in_batch = 0
    diferencias = 0

    with ruta_salida.open("w", encoding="utf-8") as out:
        for linea_a, linea_b in zip(iter_a, iter_b):
            line_number += 1
            lines_in_batch += 1

            if linea_a != linea_b:
                diferencias += 1
                out.write(f"=== Diferencia en línea {line_number} ===\n")
                out.write(f"A: {linea_a}\n")
                out.write(f"B: {linea_b}\n\n")

            # ¿terminamos un batch de 10k?
            if lines_in_batch >= BATCH_LINES:
                batch_number += 1
                lines_in_batch = 0

                # modo limitado (por ejemplo, pruebas rápidas)
                if max_batches is not None and batch_number >= max_batches:
                    break

        # Si te interesa manejar el caso en el que un archivo tiene
        # más líneas que el otro, aquí podrías añadir lógica extra.

    print(
        f"Procesadas {line_number} líneas en {batch_number} batches. "
        f"Diferencias: {diferencias}"
    )
    return ruta_salida


# ------------------------------------------------------------
# Endpoint /comparar -> quarters COMPLETOS
# ------------------------------------------------------------
@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    """
    Compara COMPLETAMENTE los archivos quarter A y B.

    Usa la misma lógica de streaming por batches, pero sin límite
    (max_batches=None), así recorre todas las líneas.
    """
    ruta = comparar_archivos_grandes(
        URL_ADN_A_QUARTER,
        URL_ADN_B_QUARTER,
        max_batches=None,  # None = procesar todo el archivo
    )

    # Leemos el reporte y lo devolvemos como texto plano
    with ruta.open("r", encoding="utf-8") as f:
        contenido = f.read()

    return contenido


# ------------------------------------------------------------
# Endpoint /comparar_grande -> archivos GRANDES
# ------------------------------------------------------------
@app.get("/comparar_grande", response_class=PlainTextResponse)
def comparar_grande(modo: str = "full"):
    """
    Compara los archivos GRANDES en modo streaming.

    - modo=full    -> recorre TODO el archivo (puede tardar varios minutos).
    - modo=render  -> procesa sólo algunos batches, útil para pruebas rápidas.
    """
    if modo == "render":
        max_batches = 10  # 10 * 10k = 100k líneas aprox.
    else:
        max_batches = None  # sin límite: todo el archivo

    ruta = comparar_archivos_grandes(
        URL_ADN_A_GRANDE,
        URL_ADN_B_GRANDE,
        max_batches=max_batches,
    )

    with ruta.open("r", encoding="utf-8") as f:
        contenido = f.read()

    return contenido


# ------------------------------------------------------------
# Root
# ------------------------------------------------------------
@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar", "/comparar_grande"],
    }
