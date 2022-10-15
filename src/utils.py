import pandas as pd
import requests
import json
import os



class BMEApiHandler:
    '''
    Descripción: Esta clase permite gestionar todas las llamadas a la API de BME. Las funciones pertenecientes
    a esta clase permiten:
    - Obtener el maestro de tickers para un mercado determinado
    - Obtener los datos de cierre para un mercado y un activo determinado
    - Obtener los datos OHLC para un mercado y un activo determinado
    - Enviar los pesos de una cartera para un mercado y un algoritmo determinado
    - Obtener los datos de los algoritmos a utilizar por un usuario en particular
    - Convertir en un dataframe un conjunto de datos en formato JSON
    - Realizar un backtesting de un algoritmo determinado para un mercado en particular
    - Obtener los resultados de un algoritmo para un mercado en particular
    - Eliminar las alocaciones realizadas para un algoritmo y un mercado en particular
    - Obtener las alocaciones realizadas para un algoritmo y un mercado en particular    
    '''
    

    def __init__(self):
        '''
        Inicializa la clase con los datos de la URL base para acceder a la API de BME, el nombre de la competencia y
        la clave de usuario.
        '''
        self.url_base = 'https://miax-gateway-jog4ew3z3q-ew.a.run.app'
        self.competi = 'mia_9'
        self.user_key = os.environ['MIAX_API_KEY']

    def get_ticker_master(self, market):
        '''
        Descripción: Esta función obtiene los datos del maestro de tickers para un mercado en particular.
        
        Parámetros:
        market (string): Es el nombre del mercado del cual se obtendrán los datos de cada ticker.
        
        Returns:
        maestro_df (DataFrame): Es un dataframe con los valores de cada ticker que exista o haya existido dentro del
        mercado indicado. Contiene los nombres de cada ticker, la fecha de entrada en el indice, la fecha de salida en caso
        de que corresponda con un activo que ya no se encuentre en el indice y la cantidad de días dentro del indice.
        '''
        # defino la URL particular para esta llamada junto con sus parámetros necesarios
        url = f'{self.url_base}/data/ticker_master'
        params = {'competi': self.competi,
                'market': market,
                'key': self.user_key}
        # realizo una llamada de tipo GET con la URL y los parámetros y la compongo en un formato JSON
        response = requests.get(url, params)
        tk_master = response.json()
        # genero un dataframe con los datos del JSON
        maestro_df = pd.DataFrame(tk_master['master'])
        return maestro_df

    def get_close_data(self, market, tck):
        '''
        Descripción: Esta funcion obtiene los datos de cierre diarios de un activo que ha estado o esta en
        el indice que se indique como parámetro.
        
        Parámetros:
        market (str): es el nombre del indice de referencia del cual se quieren obtener los datos de cierre diario.
        tck (str): es el ticker de un activo en particular del cual quieren obtenerse los datos de cierre diario.
        
        Returns:
        series_data (Series): es una serie de datos históricos de cierres diarios para el activo solicitado
        '''
        
        url = f'{self.url_base}/data/time_series'
        params = {
            'market': market,
            'key': self.user_key,
            'ticker': tck
        }
        response = requests.get(url, params)
        tk_data = response.json()
        series_data = pd.read_json(tk_data, typ='series')
        return series_data

    def get_ohlc_data(self, market, tck):
        '''
        Descripción: Esta funcion obtiene los datos de OHLCV (Open-High-Low-Close-Volume) diarios de un activo que ha estado o esta en
        el indice que se indique como parámetro.
        
        Parámetros:
        market (str): es el nombre del indice de referencia del cual se quieren obtener los datos de OHLCV diario.
        tck (str): es el ticker de un activo en particular del cual quieren obtenerse los datos de OHLCV diario.
        
        Returns:
        series_data (Series): es una serie de datos históricos de OHLCV diarios para el activo solicitado
        
        '''
        url = f'{self.url_base}/data/time_series'
        params = {
            'market': market,
            'key': self.user_key,
            'ticker': tck,
            'close': False
        }
        response = requests.get(url, params)
        tk_data = response.json()
        df_data = pd.read_json(tk_data, typ='frame')
        return df_data

    def send_alloc(self, algo_tag, market, str_date, allocation):
        '''
        Descripción: Esta función envía las alocaciones para un mercado, algoritmo y fecha en particular.
        
        algo_tag (str):
        market (str):
        
        
        '''
        
        url = f'{self.url_base}/participants/allocation'
        url_auth = f'{url}?key={self.user_key}'
        print(url_auth)
        params = {
            'competi': self.competi,
            'algo_tag': algo_tag,
            'market': market,
            'date': str_date,
            'allocation': allocation
        }
        #print(json.dumps(params))
        response = requests.post(url_auth, data=json.dumps(params))
        print(response.json())
    
    def get_algos(self):
        url = f'{self.url_base}/participants/algorithms'
        params = {'competi': self.competi,
                'key': self.user_key}
        response = requests.get(url, params)
        algos = response.json()
        if algos:
            algos_df = pd.DataFrame(algos)
            return algos_df

    def allocs_to_frame(self, json_allocations):
        alloc_list = []
        for json_alloc in json_allocations:
            #print(json_alloc)
            allocs = pd.DataFrame(json_alloc['allocations'])
            allocs.set_index('ticker', inplace=True)
            alloc_serie = allocs['alloc']
            alloc_serie.name = json_alloc['date'] 
            alloc_list.append(alloc_serie)
        all_alloc_df = pd.concat(alloc_list, axis=1).T
        return all_alloc_df

    def get_allocations(self, algo_tag, market):
        url = f'{self.url_base}/participants/algo_allocations'
        params = {
            'key': self.user_key,
            'competi': self.competi,
            'algo_tag': algo_tag,
            'market': market,
        }
        response = requests.get(url, params)
        if response.status_code == 200:
            exec_data = response.json()
            if exec_data:
                df_allocs = self.allocs_to_frame(response.json())
                return df_allocs
            else:
                exec_data = dict()
                print(response.text)

    def backtest_algo(self, algo_tag, market):
        url = f'{self.url_base}/participants/exec_algo'
        url_auth = f'{url}?key={self.user_key}'
        params = {
            'competi': self.competi,
            'algo_tag': algo_tag,
            'market': market,
            }
        response = requests.post(url_auth, data=json.dumps(params))
        if response.status_code == 200:
            exec_data = response.json()
            status = exec_data.get('status')
            res_data = exec_data.get('content')
            if res_data:
                performace = pd.Series(res_data['result'])
                trades = pd.DataFrame(res_data['trades'])
                return performace, trades
        else:
            exec_data = dict()
            print(response.text)
            
    def get_results(self,algo_tag, market):
        '''
        Descripción: Esta función obtiene los resultados de ejecutar el backtesting de un algoritmo luego de
        5 minutos de su ejecución.
        
        Parámetros:
        market (str): es el nombre del indice de referencia del cual se quieren obtener los resultados de ejecutar el backtesting
        algo_tag (str): es el nombre del algoritmo del cual se quieren obtener los resultados de ejecutar el backtesting
        
        Returns:
        performance (Series): es una serie con los datos de la performance del backesting: 
                            - Alpha de Jensen del Portfolio
                            - Ratio de Sharpe del Portfolio
                            - Rentabilidad Anualizada
                            - Cantidad de Operaciones por Año
        trades (DataFrame): es un dataframe con todos los trades realizados tanto compras como ventas de cada activo operado por el
        algoritmo seleccionado para el mercado indicado.
        '''
        url = f'{self.url_base}/participants/algo_exec_results'
        url_auth = f'{url}?key={self.user_key}'
        params = {
            'competi': self.competi,
            'algo_tag': algo_tag,
            'market': market
            }
        response = requests.get(url_auth, data=json.dumps(params))
        if response.status_code == 200:
            exec_data = response.json()
            status = exec_data.get('status')
            res_data = exec_data.get('content')
            if res_data:
                performance = pd.Series(res_data['result'])
                trades = pd.DataFrame(res_data['trades'])
                return performance,trades
        else:
            exec_data = dict()
            print(response.text)
    
    def delete_allocs(self, algo_tag, market):
        '''
        Descripción: Esta función elimina todas las alocaciones que hayan sido enviadas para un
        mercado y un algoritmo específico
        Parámetros:
        market (str): es el nombre del indice de referencia del cual se quieren eliminar todas las alocaciones.
        algo_tag (str): es el nombre del algoritmo del cual se quieren eliminar las alocaciones.
        
        Returns:
        response.text (str): indica si la respuesta de la API fue True o False.
        '''
        url = f'{self.url_base}/participants/delete_allocations'
        url_auth = f'{url}?key={self.user_key}'
        params = {
            'competi': self.competi,
            'algo_tag': algo_tag,
            'market': market,
            }
        response = requests.post(url_auth, data=json.dumps(params))
        print(response.text)
