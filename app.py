from typing import Optional
import pandas as pd
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response, JSONResponse
from datetime import date, datetime
from fastapi import Depends, HTTPException

app = FastAPI()


@app.get("/company_profiles")
async def get_company_profiles(StockCode: Optional[str] = None, Sektor: Optional[str] = None, SubSektor: Optional[str] = None):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    company_profiles = pd.read_excel('stocks.xlsx', sheet_name='Company Profiles')
    
    if StockCode:
        output_df = company_profiles[company_profiles['StockCode'] == StockCode]
    elif Sektor:
        output_df = company_profiles[company_profiles['Sektor'] == Sektor]
    elif SubSektor:
        output_df = company_profiles[company_profiles['SubSektor'] == SubSektor]
    else:
        output_df = company_profiles
    
    return Response(output_df.to_json(orient="records"), media_type="application/json")


@app.get("/trading_info")
async def get_trading_info(StockCode: str, StartDate: date, EndDate: date):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    trading_info = pd.read_excel('stocks.xlsx', sheet_name='Trading Info')

    # Filter by StockCode
    output_df = trading_info[trading_info['StockCode'] == StockCode]

    # Filter by StartDate and EndDate if both are provided
    if StartDate and EndDate:
        mask = (output_df['Date'] >= pd.to_datetime(StartDate)) & (output_df['Date'] <= pd.to_datetime(EndDate))
        output_df = output_df.loc[mask]

    return Response(output_df.to_json(orient="records"), media_type="application/json")