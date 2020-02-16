import numpy as np
import pandas as pd
import sqlalchemy as db
from src.utilities import *


class PUBLIC_LOAN_FNMA(object):
    '''processing public loan data from Fannie Mae '''

    # schema of Acquisition Data
    _AcquisitionSchema = {"LOAN_ID":         {"dtype": "string"},
                          "ORIG_CHN":        {"dtype": "string"},
                          "SellerName":      {"dtype": "string", "drop":True},
                          "ORIG_RT":         {"dtype": "float"},
                          "ORIG_AMT":        {"dtype": "double"},
                          "ORIG_TRM":        {"dtype": "int" },
                          "ORIG_DTE":        {"dtype": "date", "format":"%m/%Y", "format2":"MM/yyyy"},
                          "FRST_DTE":        {"dtype": "date", "format":"%m/%Y", "format2":"MM/yyyy"},
                          "OLTV":            {"dtype": "float", "default": 0},
                          "OCLTV":           {"dtype": "float", "default": 0},
                          "NUM_BO":          {"dtype": "int", "default": -1},
                          "DTI":             {"dtype": "int", "default": -1},
                          "CSCORE_B":        {"dtype": "float", "default": -1},
                          "FTHB_FLG":        {"dtype": "string"},
                          "PURPOSE":         {"dtype": "string"},
                          "PROP_TYP":        {"dtype": "string"},
                          "NUM_UNIT":        {"dtype": "int", "default": -1},
                          "OCC_STAT":        {"dtype": "string"},
                          "STATE":           {"dtype": "string"},
                          "ZIP_3":           {"dtype": "string"},
                          "MI_PCT":          {"dtype": "int", "default": 0},
                          "Product_Type":    {"dtype": "string"},
                          "CSCORE_C":        {"dtype": "int", "default": -1},
                          "MI_TYPE":         {"dtype": "string", "default": "0"},
                          "RELOCATION_FLG":  {"dtype": "string"}
                          }

    # schema of Performance Data
    _PerformanceSchema = {"LOAN_ID":         {"dtype": "string"},
                          "ACT_DTE":         {"dtype": "date", "format":"%d/%m/%Y", "format2":"MM/dd/yyyy"},
                          "SERVICER":        {"dtype": "string", "drop":True},
                          "LAST_RT":         {"dtype": "float"},
                          "LAST_UPB":        {"dtype": "double"},
                          "LOAN_AGE":        {"dtype": "int"},
                          "Months_To_Legal_Mat": {"dtype": "int", "default": -1},
                          "Adj_Month_To_Mat": {"dtype": "int", "default": -1},
                          "Maturity_Date":   {"dtype": "date",                      "format2":"MM/yyyy"},
                          "MSA":             {"dtype": "string", "drop":True},
                          "DLQ_STATUS":      {"dtype": "string"},
                          "MOD_FLAG":        {"dtype": "string", "drop":True},
                          "ZB_CODE":         {"dtype": "string", "drop":True},
                          "ZB_DTE":          {"dtype": "date",   "drop":True,        "format2":"MM/yyyy"    },
                          "LPI_DTE":         {"dtype": "date",   "drop":True,        "format2":"MM/dd/yyyy" },
                          "FCC_DTE":         {"dtype": "date",   "drop":True,        "format2":"MM/dd/yyyy" },
                          "DISP_DTE":         {"dtype": "date",   "drop":True,        "format2":"MM/dd/yyyy" },
                          "FCC_COST":        {"dtype": "float", "drop":True},
                          "PP_COST":         {"dtype": "float", "drop":True},
                          "AR_COST":         {"dtype": "float", "drop":True},
                          "IE_COST":         {"dtype": "float", "drop":True},
                          "TAX_COST":        {"dtype": "float", "drop":True},
                          "NS_PROCS":        {"dtype": "float", "drop":True},
                          "CE_PROCS":        {"dtype": "float", "drop":True},
                          "RMW_PROCS":       {"dtype": "float", "drop":True},
                          "O_PROCS":         {"dtype": "float", "drop":True},
                          "NON_INT_UPB":     {"dtype": "float"},
                          "PRIN_FORG_UPB_FHFA": {"dtype": "float"},
                          "REPCH_FLAG":      {"dtype": "string"},
                          "PRIN_FORG_UPB_OTH": {"dtype": "string"},
                          "TRANSFER_FLG":    {"dtype": "string"},
                          }

    def __init__(self, acqYYYYQQ, stageFolder=None,  acquisition_file=None, performance_file=None):
        self.acqYYYYQQ = acqYYYYQQ
        self.acquisition_file = acquisition_file
        self.performance_file = performance_file
        self.stageFolder = stageFolder
        self._Loan_Data = None
        self._Performance_Data = None

    @property
    def Loan_Data(self):
        return self._Loan_Data

    @Loan_Data.setter
    def Loan_Data(self, x):
        self._Loan_Data = x

    @property
    def Performance_Data(self):
        return self._Performance_Data

    @Performance_Data.setter
    def Performance_Data(self, x):
        self._Performance_Data = x

    @staticmethod
    def columnType(v):
        vout = str
        dtype = v.get('dtype')
        if dtype == "float":
            vout = np.float32
        elif dtype == "double":
            vout = np.float64
        elif dtype  == "int":
            value = v.get('default')
            if value is None:
                vout = np.int32
            else:
                vout = np.float32
        elif dtype  == "string":
            vout = str
        elif dtype == "date":
            vout = "date"
        else:
            vout = "other"
        return vout


    def read_data_acquisition(self, acquisition_file=None):
        '''
        read and pre-process acqusition data
        '''
        if acquisition_file is not None:
            self.acquisition_file = acquisition_file

        col_names = [ k for k in self._AcquisitionSchema.keys()]
        col_dtype = { k: PUBLIC_LOAN_FNMA.columnType(v) for k, v in self._AcquisitionSchema.items() \
                         if PUBLIC_LOAN_FNMA.columnType(v) not in ( "other", "date") }

        parse_dates =[ k for k, v in self._AcquisitionSchema.items() \
                         if PUBLIC_LOAN_FNMA.columnType(v) == "date" ]

        print(parse_dates)

        df = pd.read_csv(self.acquisition_file, delimiter ='|', header=None,
                    names=col_names,
                    dtype=col_dtype,  parse_dates = parse_dates
                    )
        df['OCLTV'] = df['OCLTV'].fillna(df['OLTV'])
        self.Loan_Data = df
        return None


    def read_data_performance(self, performance_file=None):
        '''
        read and pre-process performance data
        '''
        if performance_file is not None:
            self.performance_file = performance_file

        col_names = [k for k in self._PerformanceSchema.keys()]
        col_dtype = {k: PUBLIC_LOAN_FNMA.columnType(v) for k, v in self._PerformanceSchema.items() \
                     if PUBLIC_LOAN_FNMA.columnType(v) not in ("other", "date")}

        parse_dates = [k for k, v in self._PerformanceSchema.items() \
                       if PUBLIC_LOAN_FNMA.columnType(v) == "date"]

        df = pd.read_csv(self.performance_file, delimiter='|', header=None,
                         names=col_names,
                         dtype=col_dtype,
                         parse_dates=parse_dates
                         )

        for k, v in self._PerformanceSchema.items():
            value = v.get('default')
            if value is not None:
                if v.get("dtype") == "int":
                    df[k] = df[k].fillna(value).astype("int32")
                else:
                    df[k] = df[k].fillna(value)

        self.Performance_Data = df
        return None
        #df["ACQ"] = self.acqYYYYQQ
        #if self.resultFolder is not None:
        #   df.to_parquet(self.resultFolder + "\\performance.parquet", engine='pyarrow', partition_cols=['ACQ'])
      
    @decorator_time
    def read_data_performance_chunk(self, performance_file=None, **kwargs):
        '''
        read and pre-process performance data

        df.write.mode('append').parquet('parquet_data_file')

        '''
        if performance_file is not None:
            self.performance_file = performance_file

        col_names = [k for k in self._PerformanceSchema.keys()]
        col_dtype = {k: PUBLIC_LOAN_FNMA.columnType(v) for k, v in self._PerformanceSchema.items() \
                     if PUBLIC_LOAN_FNMA.columnType(v) not in ("other", "date")}

        parse_dates = [k for k, v in self._PerformanceSchema.items() \
                       if PUBLIC_LOAN_FNMA.columnType(v) == "date"]

        
        db_engine_file = 'sqlite:///' + self.stageFolder + "\\performance10.db"
        db_engine = db.create_engine(db_engine_file)

        chunksize = 10000
        loop = 0
        create_it = True
        pqwriter = None
        for df in pd.read_csv(self.performance_file, delimiter='|', header=None,
                         chunksize=chunksize, iterator=True,
                         names=col_names,
                         dtype=col_dtype,
                         parse_dates=parse_dates
                         ):
            loop = loop + 1
            print(loop)
            for k, v in self._PerformanceSchema.items():
                value = v.get('default')
                if value is not None:
                    if v.get("dtype") == "int":
                       df[k] = df[k].fillna(value).astype("int32")
                    else:
                       df[k] = df[k].fillna(value)


            print("save_sqlite")
            if create_it:
               df.to_sql("perf", db_engine, if_exists='replace')
               #table = pa.Table.from_pandas(df)
               #pqwriter = pq.ParquetWriter(self.resultFolder + "\\performance2.parquet", table.schema)
               #pqwriter.write_table(table)
               create_it = False
            else:
               df.to_sql("perf", db_engine, if_exists='append')

               #table = pa.Table.from_pandas(df)
               #pqwriter.write_table(table)
        # close the parquet writer
        if pqwriter:
            pqwriter.close()
        
        ##self.SQLite2Parquet("performance9.db",  "performance.parquet")
        return d

    def SQLite2Parquet(self, dbFile, parquetFile):
        print("SQLite2Parquet")
        db_engine_file = 'sqlite:///' + self.resultFolder + "\\" + dbFile
        db_engine = db.create_engine(db_engine_file)
        df =  pd.read_sql_query("select * from  perf ", db_engine )
        #db_engine.close()
        print("finsing reading")
        df["ACQ"] = self.acqYYYYQQ
        df.to_parquet(self.resultFolder + "\\" + parquetFile, engine='pyarrow', partition_cols=['ACQ'])
        return True

    def save_as_parquet(self, resultFolder= None, Loan_Data =True, Performance_Data=True):
       if resultFolder is not None:
            if os.path.isdir(resultFolder):
              try:
                 if (Loan_Data == True) and (self.Loan_Data is not None):
                   self.Loan_Data.write.mode('overwrite').parquet(resultFolder + "/output/FNMA/Loan.parquet/ACQ=" + self.acqYYYYQQ)
                 if (Performance_Data == True) and (self.Performance_Data is not None):
                   self.Performance_Data.write.mode('overwrite').parquet(resultFolder + "/output/FNMA/LoanPerformance.parquet/ACQ=" + self.acqYYYYQQ)
              except Exception as err:
                  print("save_as_parquet: Error:  {0}".format(err))

    def clear_data(self):
       if _Loan_Data is not None:
         del _Loan_Data
         _Loan_Data = None
       if _Performance_Data is not None:
         del _Performance_Data
         _Performance_Data = None
       
    def compute_schd_upb(self, monthCount, outAsMatrix=True, loan_Pandas_Dataframe=None):
          '''
          Calculate the scheduled UPB based on the Loan_Data["ORIG_AMT", "ORIG_RT", "ORIG_TRM"]

          Parameters
          ----------
          monthCount: int, sepcify the period to get UPB.
          outAsMatrix : default to be True, returning "matrix" or  "pandas_df"
       
          Returns
          -------
          out : ndarray (loanCount, monthCount), pandas dataframe
          '''
          if loan_Pandas_Dataframe  is None:
             sub_df = self._Loan_Data
          else:
             sub_df = loan_Pandas_Dataframe
         
          upb_matrix = compute_amortization(principals    = sub_df["ORIG_AMT"], 
                                            monthly_rates = sub_df["ORIG_RT"] / 1200,
                                            terms         = sub_df["ORIG_TRM"],
                                            start_period  = 0, 
                                            end_period    = monthCount)
          
          if outAsMatrix == True:
             return upb_matrix
          else:
             upb_array = upb_matrix.flatten()
             r,c = upb_matrix.shape
             ages_value = np.arange(c)
             ages_value = np.tile(ages_value, r)
             loanids = np.repeat(sub_df["LOAN_ID"].values, c)
             upb_array = pd.DataFrame({"LOAN_ID": loanids,
                           "LOAN_AGE": ages_value,
                           "SCHD_UPB": upb_array})
             
             return (upb_array)

