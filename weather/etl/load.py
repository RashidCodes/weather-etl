from pandas import DataFrame, read_parquet, concat
import os 


class Load():

    @staticmethod
    def load(
        df: DataFrame,
        load_target: str,
        load_method: str = "overwrite",
        target_file_directory: str = None,
        targe_file_name: str = None,
        target_database_engine = None,
        target_table_name: str = None 

    ) -> None:


        """
        Load dataframe to either a file or a database 


        Parameters
        ----------
        df: DataFrame 
            The dataframe to load 


        load_target: str 
            Choose either "file" or "database" 


        load_method: str 
            Default = "overwrite" 

            Choose either "overwrite" or "upsert". 


        target_file_directory: str 
            Data will be loaded to this directory in parquet format


        target_file_name: str 
            name of the target file e.g. stock.parquet 


        target_database_engine: 
            SQLAlchemy engine for the target database 


        target_table_name: str 
            Name of SQL Table to create and/or upsert data to 


        """ 


        if load_target.lower() == "file":

            if load_method.lower() == "overwrite":

                df.to_parquet(f"{target_file_directory}/{target_file_name}")


            if load_method.lower() == "upsert":

                if target_file_name in os.listdir(f"{target_file_directory}/"):
                    current_df = read_parquet(f"{target_file_directory}/{target_file_name}")

                    new_df = pd.concat([
                        current_df,
                        df[~df.index.isin(current_df.index)]
                    ])

                    new_df.to_parquet(f"{target_file_directory}/{target_file_name}")

                else:
                    df.to_parquet(f"{target_file_directory}/{target_file_name}")





        elif load_target.lower() == "database":

            from sqlalchemy import Table, Column, Integer, String, MetaData, Float 
            from sqlalchemy.dialects import postgresql 

            if load_method.lower() == "overwrite":
                df.to_sql(target_table_name, target_database_engine, if_exists='replace')


            elif load_method.lower() == "upsert":

                meta = MetaData()

                weather_table = Table(
                    target_table_name, meta,
                    Column("datetime", String, primary_key=True),
                    Column("id", Integer, primary_key=True),
                    Column("name", String),
                    Column("temperature", Float),
                    Column("population", Integer)
                )
                
                # create a table if it does not exists
                meta.create_all(target_database_engine)

                
                # insert statement using SQL ORM 
                insert_statement = postgresql.insert(weather_table).values(df.to_dict(orient="records"))

                # upsert 
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=["datetime", "id"],
                    set_={c.key: c for c in insert_statemenet.excluded if c.key not in ["id", "datetime"]})


                target_database_engine.execute(upsert_statement)


            else:
                raise Exception("Wrong usage of the function.")  
                






                





            
