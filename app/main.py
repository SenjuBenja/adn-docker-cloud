from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
import requests

app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases.",
    version="1.0.0",
)

# URLs de tus archivos en GitHub Release
URL_A = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
URL_B = "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"


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


@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando",
        "endpoints": ["/comparar"]
    }
