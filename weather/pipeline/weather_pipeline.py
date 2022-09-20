from database.postgres import PostgresDB
from weather.etl.extract import Extract
from weather.etl.transform import Transform
from weather.etl.load import Load 
import os 
import logging 




def pipeline() -> bool:

     # configure logging 
     logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")
     logger = logging.getLogger(__file__)
     logger.setLevel(logging.INFO)


     api_key = os.environ.get("api_key")

     logger.info("commencing extract")


     # extract 
     weather_df = Extract.extract(
         api_key=api_key,
         fp_cities="weather/data/australian_capital_cities.csv",
         temperature_units="metric"
     )


     logger.info("extract complete")


     logger.info("commencing extract from city csv")

     df_population = Extract.extract_population(fp_population="weather/data/australian_city_population.csv")

     logger.info("extract complete")


     # tranform
     logger.info("commencing transform")

     tran_df = Transform.transform(df=weather_df, df_population=df_population)
     logger.info("tranform complete")


     logger.info("commencing load to file")

     # load to file 
     Load.load(
         df = tran_df,
         load_target = "file",
         load_method = "upsert",
         target_file_directory = "weather/data",
         target_file_name = "weather.parquet"
     )

     logger.info("load to file complete")


     

     # load to database as well 
     # create engine 
     engine = PostgresDB.create_pg_engine()

     Load.load(
         df = tran_df,
         load_target = "database",
         load_method = "upsert",
         target_database_engine = engine,
         target_table_name = "weather"
     )

     logger.info("Load complete")



     return True 



if __name__ == "__main__":
     pipeline()

