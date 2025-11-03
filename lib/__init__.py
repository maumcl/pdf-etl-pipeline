import pandas as pd
import warnings
from . import legado, operadoras, df_functions, parse_file

pd.set_option("display.max_columns", 500)
warnings.filterwarnings("ignore")

parse_file.reload()