from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse
import requests
from pathlib import Path

# Pruebas dia 2 de diciembre
app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases.",
    version="1.0.0",
)

# ==============================================================
# URLs de tus archivos en GitHub Release (quarters - ENDPOINT PEQUEÑO)
# ==============================================================

URL_A = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
URL_B = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"

# ==============================================================
# URLs de ARCHIVOS GRANDES POR PARTES (1.7 GB + 1.4 GB aprox)
# ==============================================================

BASE_RELEASE = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/"

URL_A_PART1 = BASE_RELEASE + "GCA_000001405.29_GRCh38.p14--_genomic_part1.fna"
URL_A_PART2 = BASE_RELEASE + "GCA_000001405.29_GRCh38.p14--_genomic_part2.fna"

URL_B_PART1 = BASE_RELEASE + "GCF_000001405.40_GRCh38.p14--_genomic_part1.fna"
URL_B_PART2 = BASE_RELEASE + "GCF_000001405.40_GRCh38.p14--_genomic_part2.fna"

BATCH_SIZE = 5000   # Batch real para archivos grandes


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
    """Descarga los archivos pequeños y genera el reporte."""
    A = obtener_primeras_n_lineas(URL_A, n=2000)
    B = obtener_primeras_n_lineas(URL_B, n=2000)

    reporte = comparar_listas(A, B)
    return reporte


# ==============================================================
# NUEVO FLUJO PARA /comparar_grande (PARTES 1 Y 2)
# ==============================================================

def comparar_parte(url_a: str, url_b: str, out, nombre_parte: str, line_start: int):
    """
    Compara una parte (part1 o part2) usando streaming y batches.
    Escribe directamente en el archivo 'out'.
    Devuelve:
      - el nuevo número de línea (para seguir contando)
      - cantidad de diferencias encontradas en esta parte
    """
    resp_a = requests.get(url_a, stream=True)
    resp_b = requests.get(url_b, stream=True)

    if resp_a.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar A: {url_a}")
    if resp_b.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar B: {url_b}")

    iter_a = resp_a.iter_lines(decode_unicode=True)
    iter_b = resp_b.iter_lines(decode_unicode=True)

    out.write(f"########## RESULTADOS {nombre_parte} ##########\n\n")

    line_number = line_start
    diff_count = 0

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
            break  # fin de archivo de esta parte

        for i in range(max(len(batch_a), len(batch_b))):
            line_number += 1
            la = batch_a[i] if i < len(batch_a) else ""
            lb = batch_b[i] if i < len(batch_b) else ""

            if la != lb:
                diff_count += 1
                out.write(f"=== {nombre_parte} - Diferencia en línea {line_number} ===\n")
                out.write(f"A: {la}\n")
                out.write(f"B: {lb}\n\n")

    out.write(f"Total de diferencias en {nombre_parte}: {diff_count}\n\n\n")

    return line_number, diff_count


def comparar_archivos_grandes_por_partes() -> Path:
    """
    Compara:
      - Parte 1 de A vs Parte 1 de B
      - Parte 2 de A vs Parte 2 de B
    usando streaming y batches de 5000 líneas.
    Genera un único archivo reporte y devuelve su ruta.
    """
    carpeta = Path("resultados")
    carpeta.mkdir(exist_ok=True)
    salida = carpeta / "reporte_grande.txt"

    with salida.open("w", encoding="utf-8") as out:
        line_number = 0
        total_diffs = 0

        # PARTE 1
        line_number, diffs1 = comparar_parte(
            URL_A_PART1, URL_B_PART1, out, "PARTE 1", line_number
        )
        total_diffs += diffs1

        # PARTE 2
        line_number, diffs2 = comparar_parte(
            URL_A_PART2, URL_B_PART2, out, "PARTE 2", line_number
        )
        total_diffs += diffs2

        out.write("########################################\n")
        out.write(f"TOTAL DIFERENCIAS (PARTE 1 + PARTE 2): {total_diffs}\n")

    return salida


@app.get("/comparar_grande", response_class=FileResponse)
def comparar_grande():
    """
    Compara las PARTES GRANDES (part1 y part2) completamente (streaming)
    y devuelve un archivo de texto con el reporte.
    """
    ruta = comparar_archivos_grandes_por_partes()
    return FileResponse(
        ruta,
        media_type="text/plain",
        filename="reporte_grande.txt"
    )

# ==============================================================
# ROOT
# ==============================================================

@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar", "/comparar_grande"],
    }
