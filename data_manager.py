import pandas as pd

def confere_registros(entrada):
    dados = pd.read_csv('dados.csv', encoding='utf-8')

    contador = len(dados)
    entrada = dados[dados['row'] == int(entrada)]

    if not entrada.empty:
        return ['Registro existente', contador]
    else:
        return ['Novo registro', contador]