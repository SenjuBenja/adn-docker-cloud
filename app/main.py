from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse
import requests
from pathlib import Path

app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases.",
    version="1.0.0",
)

# URLs de tus archivos en GitHub Release (quarters)
URL_A = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
URL_B = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"

# URLs de archivos GRANDES (~1.2 GB)
URL_A_GRANDE = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/GCA_000001405.29_GRCh38.p14--_genomic.fna"
URL_B_GRANDE = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/GCF_000001405.40_GRCh38.p14--_genomic.fna"

BATCH_SIZE = 5000   # <<< Batch real para archivos grandes


# ==============================================================
# FUNCIONES DEL ENDPOINT ORIGINAL (NO SE TOCAN)
# ==============================================================

def obtener_primeras_n_lineas(url: str, n: int = 2000) -> list:
    """Descarga un archivo grande y devuelve sus primeras n líneas."""
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar: {url}")

    lineas = []
    for linea in resp.iter_lines():
        if linea:
            lineas.append(linea.decode("utf-8"))
        if len(lineas) >= n:
            break

    return lineas


def comparar_listas(A: list, B: list) -> str:
    """Comparación línea por línea."""
    max_len = max(len(A), len(B))
    diffs = []
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


@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    """Descarga los archivos y genera el reporte."""
    A = obtener_primeras_n_lineas(URL_A, n=2000)
    B = obtener_primeras_n_lineas(URL_B, n=2000)

    reporte = comparar_listas(A, B)
    return reporte


# ==============================================================
# NUEVO ENDPOINT /comparar_grande  (STREAMING POR BATCHES 5000)
# ==============================================================

def comparar_archivos_grandes(url_a: str, url_b: str) -> Path:
    """
    Compara dos archivos gigantes EN COMPLETO usando streaming
    y batches de 5000 líneas para evitar explosión de memoria.
    Genera un archivo reporte y devuelve su ruta.
    """
    resp_a = requests.get(url_a, stream=True)
    resp_b = requests.get(url_b, stream=True)

    if resp_a.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar A: {url_a}")
    if resp_b.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar B: {url_b}")

    iter_a = resp_a.iter_lines(decode_unicode=True)
    iter_b = resp_b.iter_lines(decode_unicode=True)

    carpeta = Path("resultados")
    carpeta.mkdir(exist_ok=True)
    salida = carpeta / "reporte_grande.txt"

    line_number = 0
    diff_count = 0

    with salida.open("w", encoding="utf-8") as out:
        while True:
            batch_a = []
            batch_b = []

            for _ in range(BATCH_SIZE):
                try:
                    batch_a.append(next(iter_a))
                    batch_b.append(next(iter_b))
                except StopIteration:
                    break

            if not batch_a and not batch_b:
                break  # fin de archivo

            # comparar este batch
            for i in range(max(len(batch_a), len(batch_b))):
                line_number += 1
                la = batch_a[i] if i < len(batch_a) else ""
                lb = batch_b[i] if i < len(batch_b) else ""

                if la != lb:
                    diff_count += 1
                    out.write(f"=== Diferencia en línea {line_number} ===\n")
                    out.write(f"A: {la}\n")
                    out.write(f"B: {lb}\n\n")

    return salida


@app.get("/comparar_grande")
def comparar_grande():
    """
    Compara los archivos GRANDES completamente (streaming).
    """
    ruta = comparar_archivos_grandes(URL_A_GRANDE, URL_B_GRANDE)
    return FileResponse(ruta, media_type="text/plain", filename="reporte_grande.txt")


# ==============================================================
# ROOT
# ==============================================================

@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar", "/comparar_grande"]
    }
