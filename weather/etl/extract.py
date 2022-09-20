from pandas import DataFrame
import pandas as pd 
import requests 



class Extract():

    @staticmethod
    def extract_city(
        api_key: str, city_name:str = None,
        temperature_units: str = "metric"
    ) -> DataFrame:

        """

        Extract data from the weather API 


        Parameters
        ----------
        api_key: str 
            API key 

        city_name: str 
            name of the city 

        temperature_units: str 
            choose one of "metric", "imperial", or "standard"




        Returns
        -------
        df: DataFrame 
            The weather data of the chose city 

        """


        params = {
            "q": city_name,
            "units": temperature_units,
            "appid": api_key 
        }

        response = requests.get(f"http://api.openweathermap.org/data/2.5/weather", params=params)

        if response.status_code == 200:
            weather_data = response.json()

            normalised = pd.normalize(weather_data)
            return normalised 

        else:
            raise Exception("Extracting weather api data faiiled") 





    @staticmethod 
    def extract(
        api_key: str,
        fp_cities: str,
        temperature_units: str = "metric"
    ) -> DataFrame:

        """ 
        Read a list of cities and get the weather 


        Parameters
        ----------
        api_key: str 
            API Key 


        fp_cities: str 
            File path of CSV containing cities 


        temperature_units:
            choose one of "metric", "imperial", or "standard"



        Returns
        -------
        weather_df: DataFrame 
            A dataframe of the weather for fp_cities

        """ 


        # read in the cities 
        cities_df = pd.read_csv(fp_cities)

        # request the data 
        weather_df = DataFrame()

        for city_name in cities_df:

            weather = Extract.extract_city(
                api_key = api_key,
                city_name = city_name,
                temperature_units = temperature_units 
            )

            weather_df = pd.concat([weather_df, weather])


        return weather_df 




    @staticmethod 
    def extract_population(fp_population: str):

        """ Extracts the population file """ 

        population_df = pd.read_csv(fp_population)

        return population_df





