from typing import Optional
import pandas as pd
from fastapi import FastAPI, APIRouter
from fastapi.responses import Response, JSONResponse, RedirectResponse
from datetime import date, datetime
from fastapi import Depends, HTTPException
import os
import sqlalchemy
from sqlalchemy import create_engine

app = FastAPI()

# Redirect root to docs
@app.get("/")
async def docs_redirect():
    return RedirectResponse(url='/docs')

engine = create_engine(
    "postgresql://{}:{}@{}/{}".format(
        os.getenv('POSTGRE_USER'), os.getenv('POSTGRE_PW'), os.getenv('POSTGRE_HOST'), os.getenv('POSTGRE_DB')
    )
)
conn = engine.connect()

@app.get("/company_profiles")
async def get_company_profiles(StockCode: Optional[str] = None, Sektor: Optional[str] = None, SubSektor: Optional[str] = None):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response
    
    if StockCode:
        output_df = pd.read_sql('''
            SELECT * FROM \"IDXCompanyProfiles\" WHERE \"IDXCompanyProfiles\".\"StockCode\" = \'{}\'
        '''.format(StockCode), con=conn)
    elif Sektor:
        output_df = pd.read_sql('''
            SELECT * FROM \"IDXCompanyProfiles\" WHERE \"IDXCompanyProfiles\".\"Sektor\" = \'{}\'
        '''.format(Sektor), con=conn)
    elif SubSektor:
        output_df = pd.read_sql('''
            SELECT * FROM \"IDXCompanyProfiles\" WHERE \"IDXCompanyProfiles\".\"SubSektor\" = \'{}\'
        '''.format(SubSektor), con=conn)
    else:
        output_df = pd.read_sql('SELECT * FROM \"IDXCompanyProfiles\"', con=conn)
    
    return Response(output_df.to_json(orient="records"), media_type="application/json")


@app.get("/trading_info")
async def get_trading_info(StockCode: str, StartDate: date, EndDate: date):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    # Filter by StockCode and Date Range
    output_df = pd.read_sql('''
        SELECT * FROM \"IDXTradingInfo\" 
        WHERE
            \"IDXCompanyProfiles\".\"StockCode\" = \'{}\' 
            AND \"IDXCompanyProfiles\".\"Date\" >= \'{}\' 
            AND \"IDXCompanyProfiles\".\"Date\" <= \'{}\'
    '''.format(StockCode, StartDate, EndDate), con=conn)

    return Response(output_df.to_json(orient="records"), media_type="application/json")

@app.get("/financial_reports")
async def get_financial_reports(StockCode: str, ReportPeriod: str, ReportYear: int):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    # Filter by StockCode and Date Range
    output_df = pd.read_sql('''
        SELECT * FROM \"IDXTradingInfo\" 
        WHERE 
            \"IDXFinancialReportLinks\".\"StockCode\" = \'{}\' 
            AND \"IDXFinancialReportLinks\".\"Report_Period\" >= \'{}\' 
            AND \"IDXFinancialReportLinks\".\"Report_Year\" <= \'{}\'
    '''.format(StockCode, ReportPeriod, ReportYear), con=conn)

    return Response(output_df.to_json(orient="records"), media_type="application/json")