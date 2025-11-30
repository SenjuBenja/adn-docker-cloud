from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
import requests
import os

app = FastAPI(
    title="API de comparación de ADN (Docker + FL0)",
    description="Compara fragmentos de dos archivos ADN alojados en GitHub Releases",
    version="1.0.0",
)


# === Utilidades para leer y comparar ===

def leer_primeras_n_lineas_desde_url(url: str, n: int = 5000) -> list[str]:
    """
    Descarga un archivo de texto grande desde una URL (GitHub Release)
    y devuelve las primeras n líneas (saltando cabeceras FASTA '>').
    """
    try:
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error al descargar desde {url}: {e}"
        )

    lineas: list[str] = []
    for raw in resp.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.strip()
        # saltar líneas de cabecera FASTA
        if line.startswith(">"):
            continue
        if line:
            lineas.append(line)
        if len(lineas) >= n:
            break

    if not lineas:
        raise HTTPException(
            status_code=500,
            detail=f"No se pudieron leer líneas de la URL {url} (¿formato vacío o solo cabeceras?)."
        )

    return lineas


def build_diff_report(lines_a: list[str], lines_b: list[str]) -> str:
    """
    Compara dos listas de líneas y genera un reporte estilo diferencias.txt.
    """
    max_lines = max(len(lines_a), len(lines_b))
    out_lines = []
    diff_count = 0

    for i in range(max_lines):
        la = lines_a[i] if i < len(lines_a) else ""
        lb = lines_b[i] if i < len(lines_b) else ""
        if la != lb:
            diff_count += 1
            out_lines.append(f"=== Diferencia en línea {i+1} ===")
            out_lines.append(f"A: {la}")
            out_lines.append(f"B: {lb}")
            out_lines.append("")

    header = [f"Total de diferencias: {diff_count}", ""]
    return "\n".join(header + out_lines)


# === Endpoints ===

@app.get("/", summary="Información básica")
def root():
    """
    Endpoint simple para verificar que el contenedor está vivo.
    """
    return {
        "message": "API de comparación de ADN (Docker + FL0)",
        "usage": "/compare?limit_lines=5000&download=1",
        "env_needed": ["ADN_URL_A", "ADN_URL_B"],
    }


@app.get(
    "/compare",
    response_class=PlainTextResponse,
    summary="Compara las secuencias y devuelve diferencias.txt"
)
def compare(limit_lines: int = 5000, download: bool = True):
    """
    Descarga dos archivos ADN desde las URLs configuradas en variables de entorno:
      - ADN_URL_A
      - ADN_URL_B

    Lee las primeras 'limit_lines' (ya sin cabeceras FASTA),
    genera un reporte de diferencias y lo devuelve como texto.
    Si 'download' es True, se envía con cabecera de archivo adjunto.
    """
    url_a = os.getenv("ADN_URL_A")
    url_b = os.getenv("ADN_URL_B")

    if not url_a or not url_b:
        raise HTTPException(
            status_code=500,
            detail="Faltan variables de entorno ADN_URL_A y/o ADN_URL_B."
        )

    lines_a = leer_primeras_n_lineas_desde_url(url_a, n=limit_lines)
    lines_b = leer_primeras_n_lineas_desde_url(url_b, n=limit_lines)

    report = build_diff_report(lines_a, lines_b)

    headers = {}
    if download:
        headers["Content-Disposition"] = 'attachment; filename="diferencias_fl0.txt"'

    return PlainTextResponse(report, headers=headers)
