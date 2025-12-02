from typing import List

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

app = FastAPI(
    title="API de comparación de ADN (Docker + FastAPI)",
    description="Compara archivos ADN descargados desde GitHub Releases (versión quarters).",
    version="1.0.0",
)

# URLs de tus archivos quarter en GitHub Releases
URL_ADN_A_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
)
URL_ADN_B_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"
)


def obtener_primeras_n_lineas(url: str, n: int = 2000) -> List[str]:
    """
    Descarga un archivo desde `url` y devuelve sus primeras `n` líneas como texto.
    """
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"No se pudo descargar: {url} (status {resp.status_code})",
        )

    lineas: List[str] = []
    for raw in resp.iter_lines():
        if not raw:
            continue
        # raw normalmente es bytes -> decodificamos a str
        try:
            linea = raw.decode("utf-8")
        except AttributeError:
            # por si alguna vez ya viene como str
            linea = str(raw)

        lineas.append(linea)
        if len(lineas) >= n:
            break

    return lineas


def comparar_listas(A: List[str], B: List[str]) -> str:
    """
    Compara dos listas de líneas y devuelve un reporte en texto plano.

    Formato:
    === Diferencia en línea X ===
    A: ...
    B: ...
    """
    max_len = max(len(A), len(B))
    diffs: List[str] = []
    cont = 0

    for i in range(max_len):
        la = A[i] if i < len(A) else ""
        lb = B[i] if i < len(B) else ""
        if la != lb:
            cont += 1
            diffs.append(f"=== Diferencia en línea {i+1} ===")
            diffs.append(f"A: {la}")
            diffs.append(f"B: {lb}")
            diffs.append("")  # línea en blanco entre diferencias

    header = f"Total de diferencias: {cont}\n\n"
    return header + "\n".join(diffs)


@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    """
    Compara las primeras 2000 líneas de los archivos quarter A y B.
    """
    A = obtener_primeras_n_lineas(URL_ADN_A_QUARTER, n=2000)
    B = obtener_primeras_n_lineas(URL_ADN_B_QUARTER, n=2000)
    reporte = comparar_listas(A, B)
    return reporte


@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando (versión quarters)",
        "endpoints": ["/comparar"],
    }
