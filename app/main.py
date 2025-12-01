from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, FileResponse

app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases.",
    version="1.0.0",
)

# ---------------------------------------------------------------------
# URLs de tus archivos en GitHub Release
# ---------------------------------------------------------------------

# QUARTERS (para /comparar)  -> OJO: pon aquí tus enlaces reales
URL_ADN_A_QUARTER = "https://github.com/tuuser/turepo/releases/download/v1/adn_quarter_A.fna"
URL_ADN_B_QUARTER = "https://github.com/tuuser/turepo/releases/download/v1/adn_quarter_B.fna"

# GRANDES (~1.2 GB) (para /comparar_grande) -> OJO: pon aquí tus enlaces reales
URL_ADN_A_GRANDE = "https://github.com/tuuser/turepo/releases/download/v1/GCA_000001405.29_GRCh38.p14--_genomic.fna"
URL_ADN_B_GRANDE = "https://github.com/tuuser/turepo/releases/download/v1/GCF_000001405.40_GRCh38.p14--_genomic.fna"

# ---------------------------------------------------------------------
# Helpers para comparación con QUARTERS (primeras 2000 líneas)
# ---------------------------------------------------------------------


def obtener_primeras_n_lineas(url: str, n: int = 2000) -> list[str]:
    """Descarga un archivo grande y devuelve sus primeras n líneas."""
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar: {url}")

    lineas: list[str] = []
    for linea in resp.iter_lines():
        if linea:
            lineas.append(linea.decode("utf-8"))
        if len(lineas) >= n:
            break

    return lineas


def comparar_listas(A: list[str], B: list[str]) -> str:
    """Comparación línea por línea en memoria (para muestras pequeñas)."""
    max_len = max(len(A), len(B))
    diffs: list[str] = []
    cont = 0

    for i in range(max_len):
        la = A[i] if i < len(A) else ""
        lb = B[i] if i < len(B) else ""
        if la != lb:
            cont += 1
            diffs.append(f"=== Diferencia en línea {i + 1} ===")
            diffs.append(f"A: {la}")
            diffs.append(f"B: {lb}")
            diffs.append("")

    header = f"Total de diferencias: {cont}\n\n"
    return header + "\n".join(diffs)


# ---------------------------------------------------------------------
# Helpers para comparación con ARCHIVOS GRANDES (streaming, batches)
# ---------------------------------------------------------------------

BATCH_LINES = 10_000  # 10k líneas por batch


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
    resp_a.raise_for_status()
    resp_b.raise_for_status()

    iter_a = resp_a.iter_lines(decode_unicode=True)
    iter_b = resp_b.iter_lines(decode_unicode=True)

    # Carpeta donde guardar el reporte
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

                # si estoy en modo limitado (Render)
                if max_batches is not None and batch_number >= max_batches:
                    break

        # Si uno tiene más líneas que otro, aquí podrías añadir lógica extra si quieres.

    print(
        f"Procesadas {line_number} líneas en {batch_number} batches. "
        f"Diferencias: {diferencias}"
    )
    return ruta_salida


# ---------------------------------------------------------------------
# ENDPOINTS
# ---------------------------------------------------------------------


@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar", "/comparar_grande"],
    }


@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    """
    Usa las primeras 2000 líneas de los QUARTERs para comparar.
    Ideal para pruebas rápidas en la nube.
    """
    A = obtener_primeras_n_lineas(URL_ADN_A_QUARTER, n=2000)
    B = obtener_primeras_n_lineas(URL_ADN_B_QUARTER, n=2000)

    reporte = comparar_listas(A, B)
    return reporte


@app.get(
    "/comparar_grande",
    summary="Comparar archivos de ADN grandes (~1.2 GB) en batches de 10k líneas",
)
def comparar_grande(
    modo: str = Query(
        "render",
        description="render = procesa sólo una parte; completo = recorre todo el archivo (usar en local)",
    )
):
    """
    Compara las dos cadenas grandes.

    - modo = 'render'   -> limita la cantidad de batches para no matar el servidor.
    - modo = 'completo' -> recorre TODO el archivo (usa esto sólo en tu máquina local).
    """

    if modo == "render":
        # Ajusta este número según lo que veas que Render aguanta
        # 200 batches * 10k líneas = 2,000,000 líneas aprox.
        max_batches = 200
    else:
        max_batches = None  # sin límite

    ruta_reporte = comparar_archivos_grandes(
        URL_ADN_A_GRANDE,
        URL_ADN_B_GRANDE,
        max_batches=max_batches,
    )

    # Devolvemos el archivo como descarga
    return FileResponse(
        path=ruta_reporte,
        media_type="text/plain",
        filename="reporte_grande.txt",
    )
