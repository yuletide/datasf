# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

# import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, ltrim, split, lit, concat

# from snowflake.snowpark.session import Session
# from snowflake.snowpark.types import IntegerType, StringType, StructType, FloatType, StructField, DateType, Variant
# from snowflake.snowpark.functions import ltrim, concat, split, udf, sum, col,array_construct,month,year,call_udf,lit
# from snowflake.snowpark.version import VERSION
# import json
# import pandas as pd
# import logging 
def model(dbt, session): 
    # https://github.com/Snowflake-Labs/sfguide-getting-started-dataengineering-ml-snowpark-python/blob/main/automate_data_pipeline_ml.py#L34
    dbt.config(materialized = "table")
    dbt.config(schema="ASR")
    dbt.config(database="TAKE_HOME_USER2")

    asr_df_table = session.table("ASR.ASR_2019")
    
    # In[17]:
    asr_df_renamed = asr_df_table.select(
      # we could also load thesse as an alias table and rename dynamically 
      col("PROPLOC").as_("Property Location"),
      col("RP1NBRCDE").as_("Assessor Neighborhood Code"),
      col("RP1PRCLID").as_("Block and Lot"),
      col("RP1VOLUME").as_("Volume Number"),
      col("RP1CLACDE").as_("Property Class Code"),
      col("YRBLT").as_("Year Property Built"),
      col("BATHS").as_("Number of Bathrooms"),
      col("BEDS").as_("Number of Bedrooms"),
      col("ROOMS").as_("Number of Rooms"),
      col("STOREYNO").as_("Number of Stories"),
      col("UNITS").as_("Number of Units"),
      col("ZONE").as_("Zoning Code"),
      col("CONSTTYPE").as_("Construction Type"),
      col("DEPTH").as_("Lot Depth"),
      col("FRONT").as_("Lot Frontage"),
      col("SQFT").as_("Property Area"),
      col("FBA").as_("Basement Area"),
      col("LAREA").as_("Lot Area"),
      col("LOTCODE").as_("Lot Code"),
      col("REPRISDATE").as_("Prior Sales Date"),
      col("RP1TRACDE").as_("Tax Rate Area Code"),
      col("OWNRPRCNT").as_("Percent of Ownership"),
      col("EXEMPTYPE").as_("Exemption Code"),
      col("RP1STACDE").as_("Status Code"),
      col("RP1EXMVL2").as_("Misc Exemption Value"),
      col("RP1EXMVL1").as_("Homeowner Exemption Value"),
      col("ROLLYEAR").as_("Closed Roll Year"),
      col("RECURRSALD").as_("Current Sales Date"),
      col("RP1FXTVAL").as_("Assessed Fixtures Value"),
      col("RP1IMPVAL").as_("Assessed Improvement Value"),
      col("RP1LNDVAL").as_("Assessed Land Value"),
      col("RP1PPTVAL").as_("Assessed Personal Property Value"),
      split(col("RP1PRCLID"), lit(" "))[0].as_("Block"),
      split(col("RP1PRCLID"), lit(" "))[1].as_("Lot"),
      concat(col("Block"), col("Lot")).as_("Parcel")
    )
    
    
    # In[18]:
    
    
    exemption_df = session.table("ASR.EXEMPTION_CODES")
    neighborhood_codes_df = session.table("ASR.NEIGHBORHOOD_CODES")
    property_codes_df = session.table("ASR.PROPERTY_CODES")
    parcels_df = session.table("ASR.PARCELS")
    
    
    # In[19]:

    asr_df_parcels = asr_df_renamed.join(
        right=parcels_df, 
        on=asr_df_renamed["parcel"] == parcels_df['"parcel_number"'], 
        how="left"
        ).select(asr_df_renamed["*"], '"analysis_neighborhood"', '"supervisor_district"')
    
    
    # In[20]:
    
    
    asr_df_exemption = asr_df_parcels.join(
        right=exemption_df,
        on=asr_df_parcels["Exemption Code"] == exemption_df['"exemption_code"'],
        how="left"
    ).select(asr_df_parcels["*"], '"exemption_definition"')
    # asr_df_exemption.show()
    
    
    # In[21]:
    
    
    # property_codes_df.show()
    asr_df_property = asr_df_exemption.join(
        right=property_codes_df,
        on=asr_df_exemption["Property Class Code"] == property_codes_df['"class_code"'],
        how="left"
    ).drop('"class_code"')
    # asr_df_property.show()
    
    
    # In[22]:
    
    

    asr_df_final = asr_df_property.join(
        right=neighborhood_codes_df,
        on=ltrim(asr_df_property["Assessor Neighborhood Code"], lit("0")) == neighborhood_codes_df['"code"'],
        how="left"
    ).select(asr_df_property["*"], '"neighborhood"')
    # asr_df_final.show()
    
    
    # In[24]:
    
    
    # asr_df_final.createOrReplaceView(f"TAKE_HOME_USER2.DBT.ASR_ANALYTICS_2019")
    # asr_df_final.write.saveAsTable("TAKE_HOME_USER2.ASR.ASR_ANALYTICS_2019")
    
    
    return asr_df_final