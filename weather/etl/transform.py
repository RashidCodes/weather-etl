from pandas import DataFrame, to_datetime



class Transform():

    @staticmethod 
    def transform(
        df: DataFrame,
        df_population: str = None
    ) -> DataFrame:

        """ 
        Transform the raw dataframe 


        Parameters
        ----------
        df: DataFrame 
            The raw dataframe 


        df_population: DataFrame 
            The dataframe produced from Extract.extract_population 



        Returns
        -------
        transformed_df: DataFrame 
            Transformed dataframe 


        """ 


        # set the city names to uppercase 
        df["city_name"] = df["name"].str.upper()

        # join df and df_population 
        merged_df = pd.merge(left=df, right=df_population, on=["city_name"])

        # select the relevant attributes 
        transformed_df = merged_df[["dt", "id", "name", "main.temp", "population"]]

        # create a new unique column id 
        transformed_df["unique_id"] = df_selected["dt"].astype(str) + df+selected["id"].astype(str)

        # convert the unix timestamp column to datetime
        transformed_df["dt"] = to_datetime(df_selected["dt"], unit="s")

        # rename columns 
        transformed_df = df_selected.rename(
            columns={
                "dt": "datetime",
                "main.temp": "temperature"
            }
        )


        # set the index 
        transformed_df = df_selected.set_inded(["unique_id"])

        return transformed_df



