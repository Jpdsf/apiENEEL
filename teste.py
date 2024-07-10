import urllib.request
import json
import pandas as pd

url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=b1bd71e7-d0ad-4214-9053-cbd58e9564a7&limit=100000'

def get_data_from_api(url):
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            return json.loads(data)
    except Exception as e:
        print(f"Erro ao acessar a API: {e}")
        return None

def process_data(data, estado=None):
    records = data['result']['records']
    df = pd.DataFrame(records)

    print("Primeiras linhas do DataFrame original:")
    print(df.head())

    df_filtered = df[['SigUF', 'DscClasseConsumo', 'MdaPotenciaInstaladaKW']].rename(columns={
        'SigUF': 'UF',
        'DscClasseConsumo': 'Classe de Consumo',
        'MdaPotenciaInstaladaKW': 'Potência Total kW'
    })

    print("Primeiras linhas do DataFrame filtrado:")
    print(df_filtered.head())

    df_filtered['Potência Total kW'] = pd.to_numeric(df_filtered['Potência Total kW'].str.replace(',', '.'), errors='coerce')

    df_filtered.dropna(subset=['Potência Total kW'], inplace=True)

    print("Primeiras linhas após conversão e remoção de NaNs:")
    print(df_filtered.head())

    if estado:
        df_filtered = df_filtered[df_filtered['UF'] == estado]
        print(f"Primeiras linhas após filtragem por estado ({estado}):")
        print(df_filtered.head())

    df_rural = df_filtered[df_filtered['Classe de Consumo'] == 'Rural']
    df_residencial = df_filtered[df_filtered['Classe de Consumo'] == 'Residencial']

    rural_by_state = df_rural.groupby('UF')['Potência Total kW'].sum().reset_index()
    residencial_by_state = df_residencial.groupby('UF')['Potência Total kW'].sum().reset_index()

    rural_by_state.rename(columns={'Potência Total kW': 'Potência Total Rural kW'}, inplace=True)
    residencial_by_state.rename(columns={'Potência Total kW': 'Potência Total Residencial kW'}, inplace=True)

    result = pd.merge(rural_by_state, residencial_by_state, on='UF', how='outer').fillna(0)

    return result

def save_to_csv(data, filename):
    data.to_csv(filename, index=False)

if __name__ == "__main__":
    data = get_data_from_api(url)
    if data:
        estado = input("Digite a sigla do estado para filtrar os dados (ou deixe em branco para todos os estados): ").upper()
        processed_data = process_data(data, estado if estado else None)

        print(processed_data)
        if not processed_data.empty:
            save_to_csv(processed_data, 'potencia_total_por_estado.csv')
            print("Dados exportados com sucesso para potencia_total_por_estado.csv")
        else:
            print("Nenhum dado encontrado para o estado especificado.")
    else:
        print("Não foi possível obter os dados da API")
