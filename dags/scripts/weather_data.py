import requests
import datetime
import pandas as pd
import uuid

api_key = "63ebf5c020bfddd664a2edf616e61988"
base_url = "http://api.openweathermap.org/data/2.5/"
save_path = ""

def get_current_weather(api_key, location):
    current_weather_url = base_url + "weather"
    params = {"appid": api_key, "q": location, "units": "metric", "lang": "pt_br"}
    response = requests.get(current_weather_url, params=params)
    return response.json()

def get_history_weather(api_key, location, date):
    current_weather = get_current_weather(api_key, location)
    if 'coord' in current_weather:
        lat = current_weather['coord']['lat']
        lon = current_weather['coord']['lon']
    else:
        return {'error': 'Não foi possível obter a coordenada da cidade'}
    
    history_weather_url = base_url + "timemachine"
    params = {"appid": api_key, "lat": lat, "lon": lon, "dt": date, "units": "metric", "lang": "pt_br"}
    response = requests.get(history_weather_url, params=params)
    return response.json()

def get_forecast_weather(api_key, location):
    forecast_weather_url = base_url + "forecast"
    params = {"appid": api_key, "q": location, "units": "metric", "lang": "pt_br"}
    response = requests.get(forecast_weather_url, params=params)
    return response.json()

def get_cities_to_weather():
    brasilian_states   = [ 'Acre', 'Alagoas', 'Amapá', 'Amazonas', 'Bahia', 'Ceará', 'Espírito Santo', 'Goiás', 'Maranhão', 'Mato Grosso', 'Mato Grosso do Sul', 'Minas Gerais', 'Pará', 'Paraíba', 'Paraná', 'Pernambuco', 'Piauí', 'Rio de Janeiro', 'Rio Grande do Norte', 'Rio Grande do Sul', 'Rondônia', 'Roraima', 'Santa Catarina', 'São Paulo', 'Sergipe', 'Tocantins' ]
    brasilian_capitals = [ 'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza',  'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina', 'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas' ]
    cities = [ x + ", " + y + ', Brazil' for x, y in zip(brasilian_capitals, brasilian_states) ]
    cities_unaccented = [ x.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ã', 'a').replace('õ', 'o').replace('ç', 'c') for x in cities]
    cities_upper = [ x.upper() for x in cities_unaccented ]
    cities = cities_upper
    return cities

def get_last_3days():
    today = datetime.datetime.now()
    last_3days = [ (today - datetime.timedelta(days=x)).timestamp() for x in range(3) ]
    return last_3days

def create_current_weather_file(cities, save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    for a in cities:
        print(a)
        record_id = uuid.uuid4()
        current_weather = get_current_weather(api_key, a)
        if current_weather and 'main' in current_weather:
            for key, value in current_weather.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        info = f"{key}_{sub_key}"
                        if info not in infos:
                            infos.append(info)
                        current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [info], 'value': [sub_value]})])
                else:
                    if key not in infos:
                        infos.append(key)
                    current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [key], 'value': [value]})])
            query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
            current_df = pd.concat([current_df, pd.DataFrame(query_info)])

    current_data_raw = current_df.pivot_table(index='record_id', columns='info', values='value', aggfunc='first').reset_index()[infos]
    current_data_raw.to_csv(save_path + 'current_raw.csv', index=False)
    current_data_raw.head()

def create_forecast_weather_file(cities, save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    for a in cities:
        print(a)
        record_id = uuid.uuid4()
        forecast_weather = get_forecast_weather(api_key, a)
        if forecast_weather and 'list' in forecast_weather:
            for forecast in forecast_weather['list']:
                for key, value in forecast.items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            info = f"{key}_{sub_key}"
                            if info not in infos:
                                infos.append(info)
                            current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [info], 'value': [sub_value]})])
                    else:
                        if key not in infos:
                            infos.append(key)
                        current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [key], 'value': [value]})])
            query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
            current_df = pd.concat([current_df, pd.DataFrame(query_info)])
        
    forecast_raw = current_df.pivot_table(index='record_id', columns='info', values='value', aggfunc='first').reset_index()[infos]
    forecast_raw.to_csv(save_path + 'forecast_raw.csv', index=False)

def create_historical_weather_file(cities, historic_weather_days = get_last_3days(), save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    days = historic_weather_days
    for day in days:
        for a in cities:
            print(datetime.datetime.fromtimestamp(day).strftime('%Y-%m-%d') + " - " + a)
            record_id = uuid.uuid4()
            history_weather = get_history_weather(api_key, a, int(day))
            if history_weather and 'hourly' in history_weather:
                for hour_data in history_weather['hourly']:
                    for key, value in hour_data.items():
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                info = f"{key}_{sub_key}"
                                if info not in infos:
                                    infos.append(info)
                                current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [info], 'value': [sub_value]})])
                        else:
                            if key not in infos:
                                infos.append(key)
                            current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [key], 'info': [key], 'value': [value]})])
            query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
            current_df = pd.concat([current_df, pd.DataFrame(query_info)])
            
    history_raw = current_df.pivot_table(index='record_id', columns='info', values='value', aggfunc='first').reset_index()
    history_raw.to_csv(save_path + 'history_raw.csv', index=False)

if __name__ == "__main__":
    cities = get_cities_to_weather()
    create_current_weather_file(cities)
    create_forecast_weather_file(cities)
    create_historical_weather_file(cities)
