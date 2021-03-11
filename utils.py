import json
import requests
import gzip
import io
from datetime import datetime
import pandas as pd
import numpy as np


def make_request(url:str) -> pd.core.frame.DataFrame:
    """
    Função que acessa uma url, copia seu conteudo, descomprime a informação em gzip para um json, e cria um Pndas Dataframe com esse json

    Parâmetros:
    url [str] - a url que será feita a requisição
    
    Retorna:
    Um Pandas Dataframe com as informações extraídas da requisição
    """

    response = requests.get (url)

    if response.status_code == 200:

        gzip_file = io.BytesIO(response.content)

        with gzip.open (gzip_file, 'rt') as f:
            json_data = f.read()

        df = pd.read_json(
            path_or_buf=io.StringIO(json_data),
            orient='records', 
            lines=True
        )

        return df

    return pd.DataFrame()


def check_session_id(row:pd.core.series.Series) -> int:
    """
    Função que retorna o valor 1 se a row for a primeira de um anonymous_id ou se há uma diferença maior que 30 minutos entre o registro atual e o último.
    Como o dataframe foi ordenado por anonymous_id e device_sent_timestamp, sempre o primeiro registro que configura uma sessão ficará com o valor 1 e os outros registros
    ficarão com o valor 0

    Parâmetros:
    row [pd.core.series.Series] - o row de um Pandas Dataframe
    
    Retorna:
    Uma valor int que diz se é o primeiro registro de uma session_id ou não
    """

    if  (row['last_anonymous_id'] == 0) or \
        (row['anonymous_id'] != row['last_anonymous_id']) or \
        (pd.to_datetime(row['device_sent_timestamp']) + pd.offsets.Minute(30) < pd.to_datetime(row['last_device_sent_timestamp'])):        
        return 1
    return 0


def calculate_session_time(row:pd.core.series.Series) -> float:
    """
    Função que retorna a diferença em segundos entre os dois timestamps da row. Essa diferença caracteriza o tempo de sessão

    Parâmetros:
    row [pd.core.series.Series] - o row de um Pandas Dataframe
    
    Retorna:
    Uma valor float que significa o tempo da sessão
    """
    return float((row['device_sent_timestampmax'] - row['device_sent_timestampmin']) / 1000)


def generate_session_from_df(df:pd.core.frame.DataFrame, session_id_starter:int) -> pd.core.frame.DataFrame:
    """
    Função que transforma um dataframe cru em um dataframe agrupado por uma session_id
    Primeiramente ele ordena os registros.
    Em seguida ele inclui duas novas colunas com informações da coluna anterior
    Em seguida é usada essas colunas para construir o session_id
    Pot último ele transforma o dataframe agrupando pelo session_id com o tempo de sessão calculado

    Parâmetros:
    df [pd.core.frame.DataFrame] - um Pandas Dataframe a ser transformado
    session_id_starter [int] - usado no caso de haver a necessidade de se juntar vários dataframes diferentes, a fim da session_id não ser repetida
    
    Retorna:
    Um Pandas Dataframe transformado com session_id e calculado o tempo de sessão
    """

    #O dataframe é sorteado aqui pela anonymous_id e device_sent_timestamp
    df_sorted = df.sort_values(by=['anonymous_id','device_sent_timestamp'])

    """
    Iterar um pandas dataframe com um for loop é muito custoso
    A tentativa aqui é não usar um for loop e apelar para vetorização.
    Esse primeiro passo se trata de criar colunas de last_anonymous_id e last_device_sent_timestamp, para que
    ao fazer as comparações no dataframe eles sejam sempre feitas numa mesma linha, de forma vetorizada.

    |anonymous_id|device_sent_timestamp|           |anonymous_id|device_sent_timestamp|last_anonymous_id|last_device_sent_timestamp|
    |    1       |    1592608047794    |           |    1       |    1592608047794    |       0         |           0              |
    |    2       |    1592608047795    |  -->>     |    2       |    1592608047795    |       1         |      1592608047794       |
    |    3       |    1592608047796    |           |    3       |    1592608047796    |       2         |      1592608047795       |
    |    4       |    1592608047797    |           |    4       |    1592608047797    |       3         |      1592608047796       |
    """
    array_anonymous = np.insert(df_sorted['anonymous_id'].to_numpy(), 0, '', axis=0)
    array_device_sent_timestamp = np.insert(df_sorted['device_sent_timestamp'].to_numpy(), 0, 0, axis=0)
    df_sorted['last_anonymous_id'] = array_anonymous[:-1]
    df_sorted['last_device_sent_timestamp'] = array_device_sent_timestamp[:-1]    

    """
    Nesse momento é feito um novo campo que irá sinalizar qual o primeiro registro de cada session_id
    Como eles estão ordenados pelo anonymous_id e device_sent_timestamp, essa lógica funcionará até o final
    sem precisar olhar para os rows proximos ou anterior

    |anonymous_id|device_sent_timestamp|last_anonymous_id|last_device_sent_timestamp|session_id_bool|
    |    1       |    1592608047794    |       0         |           0              |      1        |
    |    1       |    1592608047795    |       1         |      1592608047794       |      0        |
    |    3       |    1592608047796    |       1         |      1592608047795       |      1        |
    |    3       |    1592608047797    |       3         |      1592608047796       |      0        |
    """
    df_sorted['session_id_bool'] = df_sorted.apply(lambda x: check_session_id(x), axis = 1)

    """
    Nesse momento é usado uma função de agrupamento juntamento com uma soma acumulativa.
    Lembra que somente o primeiro row de cada sessão tem o valor um no campo session_id_bool? 
    Imagine que em cada linha ele irá somar  de forma acumulada, logo ele somente irá mudar 
    de valor quando achar uma outra session_id, pois os outros registros tem valor zero.

    |anonymous_id|device_sent_timestamp|last_anonymous_id|last_device_sent_timestamp|session_id_bool|session_id|
    |    1       |    1592608047794    |       0         |           0              |      1        |    1     |
    |    1       |    1592608047795    |       1         |      1592608047794       |      0        |    1     |
    |    3       |    1592608047796    |       1         |      1592608047795       |      1        |    2     |
    |    3       |    1592608047797    |       3         |      1592608047796       |      0        |    2     |
    """    
    df_sorted['label'] = ''
    df_sorted['session_id']=df_sorted.groupby('label')['session_id_bool'].apply(lambda x:(x.cumsum()+session_id_starter))

    """
    Pronto session_id criada! Agora temos que agrupar pelo session_id e calcular o valor minimo e maximo do timestamp
    para podermos calcular o tempo de sessão que é entre o valor inicial e final deles

    |session_id|device_sent_timestampmin|device_sent_timestampmax|session_time|
    |    1     |    1592608047794       |       1592608047795    |    0.001   |
    |    2     |    1592608047796       |       1592608047797    |    0.001   |
    """ 
    df_session = df_sorted.groupby(['session_id','browser_family','os_family','device_family'], as_index=False).agg({'device_sent_timestamp': ['min', 'max']}) 
    df_session.columns = list(map(''.join, df_session.columns.values))
    df_session['session_time'] = df_session.apply(lambda x: calculate_session_time(x), axis = 1)

    return df_session