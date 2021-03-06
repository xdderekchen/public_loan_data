from datetime import datetime

from pyspark.sql import functions as F,  Window
from pyspark.sql.types import DateType, StructType, StructField,  DoubleType, FloatType, IntegerType, LongType, StringType, DateType
import numpy as np
import pandas as pd
import sqlalchemy as db
from src.utilities import *
import time
from src.public_loan_fnma import *

class PUBLIC_LOAN_FNMA_spark(PUBLIC_LOAN_FNMA):
    '''processing public loan data from Fannie Mae '''
      #schema of Acquisition Data
    
    def __init__(self, acqYYYYQQ, stageFolder=None,  acquisition_file=None, performance_file=None):
         super().__init__(acqYYYYQQ, stageFolder    , acquisition_file, performance_file)
         

    @staticmethod
    def convTypeToSpark(v):
          vout = StringType()
          dtype = v.get('dtype')
          if dtype == "float":
             vout = FloatType()
          elif dtype == "double":
            vout = DoubleType()
          elif dtype  == "int":
            vout = IntegerType()
          elif dtype  == "string":
            vout = StringType()
          elif dtype == "date":
            vout = StringType()
          else:
            vout = StringType()
          return vout

    @staticmethod
    def save_to_hive(dbname, tablename, dataframe_src):
          #1. Creating Hive Database
          spark.sql('create database IF NOT EXISTS ' + dbname)
          spark.sql('use ' + dbname )
          spark.sql("drop table IF  EXISTS " + tablename)                                   
          spark.sql("show tables").show()

          spark.sql("insert into table ratings \
                     select * from " +dataframe_src )

    def compute_schd_upb(self, monthCount, outAsMatrix=True):
          '''
          Calculate the scheduled UPB based on the Loan_Data["ORIG_AMT", "ORIG_RT", "ORIG_TRM"]

          Parameters
          ----------
          monthCount: int, sepcify the period to get UPB.
          outformat : string, "matrix", "pandas_df", "spark_df"
       
          Returns
          -------
          out : ndarray (M, N),  spark dataframe
          '''
          sub_df = self.Loan_Data.select("LOAN_ID", "ORIG_AMT", "ORIG_RT", "ORIG_TRM").toPandas()
         
          upb_matrix = super().compute_schd_upb(monthCount, outAsMatrix, sub_df)
          
          if outAsMatrix == True:
             return upb_matrix
          else:
             return spark.createDataFrame(upb_matrix)
        


    def read_data_acquisition (self, spark, acquisition_file=None):
          '''
          read and pre-process acqusition data
          '''
          if acquisition_file is not None:
             self.acquisition_file = acquisition_file

          # need to check file existing
          ColumnSchema = StructType([StructField(k, PUBLIC_LOAN_FNMA_spark.convTypeToSpark(v), True) for k, v in self._AcquisitionSchema.items()])                   
          ts = time.time()
          acq_df = spark.read.format("csv").options(header='False', delimiter="|").schema(ColumnSchema).load(self.acquisition_file )
          
          for k, v in self._AcquisitionSchema.items():
             if v.get('dtype') == "date":
                acq_df = acq_df.withColumn(k,  F.to_date(F.col(k), v.get('format2')))
             else:
                value = v.get('default')
                if value is not None:
                   acq_df = acq_df.withColumn(k, F.when(F.col(k).isNull(), value).otherwise(F.col(k)))

          acq_df = acq_df.withColumn('OCLTV',     F.expr("case when OCLTV ==0 then OLTV else OCLTV end"))

          for k, v in self._AcquisitionSchema.items():
             if v.get('drop') == True:
                acq_df= acq_df.drop(k)
         
          self.Loan_Data = acq_df
          
          ts = time.time()
          schd_upbData = self.compute_schd_upb(monthCount=12, outAsMatrix=False)
          print("compute upb: ", showtime(ts))
        
          return None

    def read_data_performance (self, spark, performance_file=None):
          '''
          read and pre-process acqusition data
          '''
          if performance_file is not None:
             self.performance_file = performance_file

          # need to check file existing
          ColumnSchema = [StructField(k, PUBLIC_LOAN_FNMA_spark.convTypeToSpark(v), True) for k, v in self._PerformanceSchema.items()]
          ColumnSchema = StructType(ColumnSchema)                                

          #udf
          #str_to_date =  udf (lambda x: datetime.strptime(x, '%m/%Y'), DateType())   
          windowIncF12 =  Window().partitionBy("LOAN_ID").orderBy("ACT_DTE").rowsBetween(1, 12)
          windowIncF24 =  Window().partitionBy("LOAN_ID").orderBy("ACT_DTE").rowsBetween(1, 24)
          windowIncP12 =  Window().partitionBy("LOAN_ID").orderBy("ACT_DTE").rowsBetween(-12, -1)
          windowInc    =  Window().partitionBy("LOAN_ID").orderBy("ACT_DTE")

          ts = time.time()
          per_df = spark.read.format("csv").options(header='False', delimiter="|").schema(ColumnSchema).load(self.performance_file )
          #per_df.show(10)
          print("read cvs: ", showtime(ts))
        
          ts = time.time()
          # date columnd
          for k, v in self._PerformanceSchema.items():
             if v.get('dtype') == "date":
                per_df = per_df.withColumn(k,  F.to_date(F.col(k), v.get('format2')))

          per_df = per_df.withColumn('DLQ_STATUS', F.when(F.col('DLQ_STATUS').isNull(), -1).when(F.col('DLQ_STATUS') == "X", -2).otherwise(F.col('DLQ_STATUS').cast(IntegerType())))
          per_df = per_df.withColumn('DEFT_COST',  F.expr("FCC_COST + PP_COST + AR_COST + IE_COST + TAX_COST"))
          per_df = per_df.withColumn('DEFT_PROCS',  F.expr("NS_PROCS + CE_PROCS + RMW_PROCS + O_PROCS"))

          per_df = per_df.withColumn("DLQ_LAG", F.lag('DLQ_STATUS', 1).over(windowInc)) \
              .withColumn("DLQ_LAGs12",         F.collect_list('DLQ_STATUS').over(windowIncP12)) \
              .withColumn("DLQ_NEXT12MAX",      F.max('DLQ_STATUS').over(windowIncF12)) \
              .withColumn("DLQ_NEXT24MAX",      F.max('DLQ_STATUS').over(windowIncF24)) \
              .withColumn("ZBCODE_NEXT12",      F.max('ZB_CODE').over(windowIncF12)) \
              .withColumn("ZBCODE_NEXT24",      F.max('ZB_CODE').over(windowIncF24)) \
              .withColumn("MOD_NEXT12",         F.max('MOD_FLAG').over(windowIncF12)) \
              .withColumn("MOD_NEXT24",         F.max('MOD_FLAG').over(windowIncF24)) 

          print(per_df.printSchema())
          
          print("transform: ", showtime(ts))
        
          ts = time.time()
          Mod_DF = per_df.filter(F.col("MOD_FLAG")=='Y').withColumn("rn", F.row_number().over(windowInc)).where((F.col("rn") ==1)) \
                      .select("LOAN_ID", 
                              F.col("ACT_DTE").alias("MOD_DTE"), 
                              F.col("LOAN_AGE").alias("MOD_AGE"), 
                              F.col("DLQ_STATUS").alias("MOD_DLQ"),
                              F.col("DLQ_LAG").alias("MOD_DLQ_LAG"),
                              F.col("DLQ_LAGs12").alias("MOD_DLQ_LAGs12"),
                              F.col("DLQ_NEXT12MAX").alias("MOD_POST_MAXDLQ_12"),
                              F.col("DLQ_NEXT24MAX").alias("MOD_POST_MAXDLQ_24"),
                              F.col("ZBCODE_NEXT12").alias("MOD_POST_ZBCODE_12"),
                              F.col("ZBCODE_NEXT24").alias("MOD_POST_ZBCODE_24")
                              )
          print("Mod DF: ", showtime(ts))
        
          ts = time.time()
          F3Q_DF = per_df.filter(F.col("DLQ_STATUS")>2).withColumn("rn", F.row_number().over(windowInc)).where((F.col("rn") ==1)) \
                     .select("LOAN_ID", 
                              F.col("ACT_DTE").alias("F3Q_DTE"), 
                              F.col("LOAN_AGE").alias("F3Q_AGE"), 
                              F.col("DLQ_LAGs12").alias("F3Q_DLQ_LAGs12"),
                              F.col("DLQ_NEXT12MAX").alias("F3Q_POST_MAXDLQ_12"),
                              F.col("DLQ_NEXT24MAX").alias("F3Q_POST_MAXDLQ_24"),
                              F.col("ZBCODE_NEXT12").alias("F3Q_POST_ZBCODE_12"),
                              F.col("ZBCODE_NEXT24").alias("F3Q_POST_ZBCODE_24")
                              )
          print("F3Q DF: ", showtime(ts))
        
          ts = time.time()
          ZB_DF = per_df.filter(F.col("ZB_CODE").isNotNull()) \
                    .select("LOAN_ID", "ZB_CODE", "ZB_DTE",
                              F.col("LOAN_AGE").alias("ZB_AGE"), 
                              F.col("DLQ_LAGs12").alias("ZB_DLQ_LAGs12"),
                              F.col("DLQ_LAG").alias("ZB_DLQ_LAG"),
                              F.col("LAST_UPB").alias("ZB_LAST_UPB"),
                              "LPI_DTE",
                              "FCC_DTE",
                              "DISP_DTE", "DEFT_COST", "DEFT_PROCS"
                    )
          print("ZB DF: ", showtime(ts))
        
          ts = time.time()

          if self.Loan_Data is None:
             loanLevelDF =  Mod_DF.select("LOAN_ID") \
                     .union(F3Q_DF.select("LOAN_ID")) \
                     .union( ZB_DF.select("LOAN_ID")).distinct()
          
             loanLevelDF = loanLevelDF.join(Mod_DF, "LOAN_ID", how="left")
             loanLevelDF = loanLevelDF.join(F3Q_DF, "LOAN_ID", how="left")
             loanLevelDF = loanLevelDF.join(ZB_DF,  "LOAN_ID",  how="left") 
          else:
             self.Loan_Data = self._Loan_Data.join(Mod_DF, "LOAN_ID", how="left") \
                                         .join(F3Q_DF, "LOAN_ID", how="left") \
                                         .join(ZB_DF,  "LOAN_ID",  how="left").orderBy(["LOAN_ID"])
              
          print("join: ", showtime(ts))
          select_col = [k for k, v in self._PerformanceSchema.items() if v.get("drop", False) == False]
          Performance_Data = per_df.select(select_col)

          #calcualte schd_upb
          age_max = 12  #Performance_Data.agg({"LOAN_AGE": "max"}).first()[0]
          ts = time.time()
          schd_upbData = self.compute_schd_upb(monthCount=age_max, outAsMatrix=False)
          print("compute upb: ", showtime(ts))
         
          ts = time.time()
          self.Performance_Data = Performance_Data.join(schd_upbData, on=["LOAN_ID", "LOAN_AGE"], how="left").orderBy(["LOAN_ID",  "ACT_DTE"])
          print("joining upb: ", showtime(ts))
          ts = time.time()
          self.Performance_Data = self.Performance_Data.filter(F.col("LOAN_AGE")>=0).withColumn("LAST_UPB",  F.expr("case when LAST_UPB is Null then round(SCHD_UPB,3) else LAST_UPB end") )
          self.Performance_Data=  self.Performance_Data.drop("SCHD_UPB")
          print("filtering out: ", showtime(ts))
           
          return None
