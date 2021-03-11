import json
from datetime import datetime
import pandas as pd
from utils import generate_session_from_df,make_request


def main():

    #iniciando variáveis
    df_agregated = ''
    session_id_starter = 0
    result = {}

    for n in range(10):

        url = f"https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-{str(n).zfill(5)}.json.gz"
        df = make_request(url)

        #função que prepara o dataframe com sessionamento
        df_session = generate_session_from_df(
            df,
            session_id_starter
        )

        #como estamos iterando concatena os dataframes caso não seja o primeiro da iteração
        if (len(df_agregated) > 0):            
            df_agregated = pd.concat([df_session, df_agregated])            
        else:
            df_agregated = df_session
        
        #atualiza o valor de session_id_starter para que sempre na proxima iteração ele comece do tamanho do dataframe atual + 1
        session_id_starter = len(df_agregated)
 
    #calcula a mediana por segmento e organiza em um Dict, para no final imprimir em json
    for segmento in ['browser_family','os_family','device_family']:
        df_mediana = df_agregated.groupby([segmento], as_index=False)[['session_time']].median()
        result_df_mediana = {}
        for index, row in df_mediana.iterrows():
            result_df_mediana[row[segmento]] = row['session_time']

        result[segmento] = result_df_mediana

    print(json.dumps(result))


if __name__ == "__main__":
    main()