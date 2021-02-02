import pandas as pd
import os


def get_cities_data():
    pd.set_option('display.max_columns', 500)

    pth = './cities_data/worldcities.csv'
    if not os.path.isfile(pth):
        os.makedirs('./cities_data', exist_ok=True)
        os.system(
            'wget https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.73.zip -O ./cities_data/cities.zip')
        os.system('unzip ./cities_data/cities.zip -d ./cities_data')
    return pd.read_csv(pth)


def get_random_city(df):
    """
    returns an info dict for a random city containing

        {city_name: "São Paulo", city_ascii: "Sao Paulo",
         lat: -23.5504, lng: -46.6339, country: "Brazil", iso2: "BR", iso3: "BRA",
         admin_name: "São Paulo", capital: "admin", population: 22046000.0, id: 1076532519}

    :param df: pandas df containing cities data from simplemaps.com
    :return:
    """
    return df.sample().to_dict('records')[0]


def get_city(df, city_name=None, city_index=None):
    """
    returns an info dict for a city specified by `city name` containing

        {city_name: "São Paulo", city_ascii: "Sao Paulo",
         lat: -23.5504, lng: -46.6339, country: "Brazil", iso2: "BR", iso3: "BRA",
         admin_name: "São Paulo", capital: "admin", population: 22046000.0, id: 1076532519}

    :param df: pandas df containing cities data from simplemaps.com
    :return:
    """
    assert any([city_name is not None, city_index is not None])
    if city_name is not None:
        return df.loc[df['city_ascii'].str.lower() == city_name.lower()].to_dict('records')[0]
    if city_index is not None:
        return df.iloc[[2]].to_dict('records')[0]



if __name__ == "__main__":
    df = get_cities_data()
    print(get_random_city(df))
