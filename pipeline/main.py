from funciones import *
from atributos import *
import pandas as pd

pd.pipe(data_load(archivos)).pipe(data_cleaning()).pipe(data_analysis()).pipe(data_type_mod())
