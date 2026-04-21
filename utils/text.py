import pandas as pd

def normalize(text):
    if pd.isna(text):
        return ""
    return str(text).strip().lower()