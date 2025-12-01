from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse

# ------------------------------------------------------------
# Configuración de la API
# ------------------------------------------------------------
app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases o desde archivos grandes.",
    version="1.0.0",
)

# ------------------------------------------------------------
# CONSTANTES: PON AQUÍ TUS URLs REALES
# ------------------------------------------------------------

# 🔹 URLs de los archivos "pequeños" (quarters o recortes)
URL_ADN_A_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
)
URL_ADN_B_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"
)

# 🔹 URLs de los archivos GRANDES (los ~1.2 GB que cortaste)
URL_ADN_A_GRANDE = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/GCA_000001405.29_GRCh38.p14--_genomic.fna"
)
URL_ADN_B_GRANDE = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/GCF_000001405.40_GRCh38.p14--_genomic.fna"
)

# LÍNEAS POR BATCH para comparación grande
BATCH_LINES = 10_000  # 10k líneas


# ------------------------------------------------------------
# Utilidades para archivos "pequeños" (primeras N líneas)
# ------------------------------------------------------------
def obtener_primeras_n_lineas(url: str, n: int = 2000) -> list[str]:
    """Descarga un archivo y devuelve sus primeras n líneas (texto)."""
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(
            status_code=500, detail=f"No se pudo descargar: {url} (status {resp.status_code})"
        )

    lineas: list[str] = []
    for raw in resp.iter_lines():
        if not raw:
            continue
        lineas.append(raw.decode("utf-8"))
        if len(lineas) >= n:
            break

    return lineas


def comparar_listas(A: list[str], B: list[str]) -> str:
    """Compara dos listas línea por línea y devuelve un reporte en texto."""
    max_len = max(len(A), len(B))
    diffs: list[str] = []
    cont = 0

    for i in range(max_len):
        la = A[i] if i < len(A) else ""
        lb = B[i] if i < len(B) else ""
        if la != lb:
            cont += 1
            diffs.append(f"=== Diferencia en línea {i+1} ===")
            diffs.append(f"A: {la}")
            diffs.append(f"B: {lb}")
            diffs.append("")

    header = f"Total de diferencias: {cont}\n\n"
    return header + "\n".join(diffs)


# ------------------------------------------------------------
# Endpoint /comparar -> usa solo una parte (2000 líneas)
# ------------------------------------------------------------
@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    """
    Compara las primeras 2000 líneas de los archivos A y B.
    Útil para pruebas rápidas en Render.
    """
    A = obtener_primeras_n_lineas(URL_ADN_A_QUARTER, n=2000)
    B = obtener_primeras_n_lineas(URL_ADN_B_QUARTER, n=2000)

    reporte = comparar_listas(A, B)
    return reporte


# ------------------------------------------------------------
# Utilidades para archivos GRANDES (streaming por batches)
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

    - max_batches = None  -> recorre TODO el archivo (modo 'completo', ideal en local).
    - max_batches = N     -> procesa sólo N batches (N * 10k líneas), útil para Render.
    """

    resp_a = requests.get(url_a, stream=True)
    resp_b = requests.get(url_b, stream=True)

    if resp_a.status_code != 200:
        raise HTTPException(
            status_code=500, detail=f"No se pudo descargar A: {url_a} (status {resp_a.status_code})"
        )
    if resp_b.status_code != 200:
        raise HTTPException(
            status_code=500, detail=f"No se pudo descargar B: {url_b} (status {resp_b.status_code})"
        )

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

                # modo limitado (por ejemplo, Render)
                if max_batches is not None and batch_number >= max_batches:
                    break

        # Aquí podrías agregar lógica para cuando uno de los archivos
        # tiene más líneas que el otro, si lo necesitas.

    print(
        f"Procesadas {line_number} líneas en {batch_number} batches. "
        f"Diferencias: {diferencias}"
    )
    return ruta_salida


# ------------------------------------------------------------
# Endpoint /comparar_grande -> streaming por batches
# ------------------------------------------------------------
@app.get("/comparar_grande")
def comparar_grande(modo: str = "render"):
    """
    Compara los archivos GRANDES en modo streaming.

    - modo=render  -> procesa p.ej. 10 batches de 10k líneas (100k líneas).
    - modo=completo -> recorre TODO el archivo (mejor hacerlo en local).
    """
    if modo == "render":
        max_batches = 10  # 10 * 10k = 100k líneas
    else:
        max_batches = None  # sin límite

    ruta = comparar_archivos_grandes(
        URL_ADN_A_GRANDE,
        URL_ADN_B_GRANDE,
        max_batches=max_batches,
    )

    # Devolvemos el archivo como descarga
    return FileResponse(
        ruta,
        media_type="text/plain",
        filename=ruta.name,
    )


# ------------------------------------------------------------
# Root
# ------------------------------------------------------------
@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar", "/comparar_grande"],
    }
