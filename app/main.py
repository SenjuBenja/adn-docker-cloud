from typing import List

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

app = FastAPI(
    title="API ADN Docker (quarters 2000 líneas)",
    description="Compara las primeras 2000 líneas de los archivos quarter A y B desde GitHub Releases.",
    version="1.0.0",
)

URL_ADN_A_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_A.fna"
)
URL_ADN_B_QUARTER = (
    "https://github.com/SenjuBenja/adn-docker-cloud/releases/download/v1.0.0/adn_quarter_B.fna"
)


def obtener_primeras_n_lineas(url: str, n: int = 2000) -> List[str]:
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
        try:
            linea = raw.decode("utf-8")
        except AttributeError:
            linea = str(raw)

        lineas.append(linea)
        if len(lineas) >= n:
            break

    return lineas


def comparar_listas(A: List[str], B: List[str]) -> str:
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
            diffs.append("")

    header = f"Total de diferencias: {cont}\n\n"
    return header + "\n".join(diffs)


@app.get("/comparar", response_class=PlainTextResponse)
def comparar():
    A = obtener_primeras_n_lineas(URL_ADN_A_QUARTER, n=2000)
    B = obtener_primeras_n_lineas(URL_ADN_B_QUARTER, n=2000)
    reporte = comparar_listas(A, B)
    return reporte


@app.get("/")
def root():
    return {
        "mensaje": "API ADN Docker funcionando (quarters 2000 líneas)",
        "endpoints": ["/comparar"],
    }
