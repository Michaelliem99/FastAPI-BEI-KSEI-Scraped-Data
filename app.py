from typing import Optional
import pandas as pd
from fastapi import FastAPI, APIRouter, Query
from fastapi.responses import Response, JSONResponse, RedirectResponse
from datetime import date, datetime
from fastapi import Depends, HTTPException
import os
import sqlalchemy
from sqlalchemy import create_engine
from fastapi.openapi.utils import get_openapi

app = FastAPI()

def my_schema():
   openapi_schema = get_openapi(
       title="API for IDX and KSEI Stock and Bond Data",
       version="1.0",
       description="API to access my IDX and KSEI data scraping results in PostgreSQL Neon Tech DB.",
       routes=app.routes,
   )
   openapi_schema["info"] = {
       "title" : "API for IDX and KSEI Stock and Bond Data",
       "version" : "1.0",
       "description" : "API to access my IDX and KSEI data scraping results.",
       "contact": {
           "name": "API Github",
           "url": "https://github.com/Michaelliem99/FastAPI-BEI-KSEI-Scraped-Data",
       }
   }
   app.openapi_schema = openapi_schema
   return app.openapi_schema

app.openapi = my_schema
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

@app.get("/api_stock_query_options")
async def get_stock_query_options():
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    with engine.connect() as conn:
        StockCodeDF = pd.read_sql('SELECT DISTINCT \"StockCode\" FROM \"IDXCompanyProfiles\"', con=conn)
        SectorDF = pd.read_sql('SELECT DISTINCT \"Sektor\" FROM \"IDXCompanyProfiles\"', con=conn)
        SubSectorDF = pd.read_sql('SELECT DISTINCT \"SubSektor\" FROM \"IDXCompanyProfiles\"', con=conn)

    JSONOutput = {
        'StockCodeList':StockCodeDF['StockCode'].to_list(),
        'SectorList':SectorDF['Sektor'].to_list(),
        'SubSectorList':SubSectorDF['SubSektor'].to_list()
    }
  
    return JSONOutput

@app.get("/company_profiles")
async def get_company_profiles(
    StockCode: Optional[str] = Query(None, description='IDX Stock Code (e.g., BBCA, ABBA, GOTO), available options can be viewed through "/api_stock_query_options" endpoint'), 
    Sektor: Optional[str] = Query(None, description='IDX Stock Sector Name (e.g., Keuangan, Perindustrian), available options can be viewed through "/api_stock_query_options" endpoint'), 
    SubSektor: Optional[str] = Query(None, description='IDX Stock Sub-Sector Name (e.g., Asuransi, Perdagangan Ritel), available options can be viewed through "/api_stock_query_options" endpoint')
):
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    with engine.connect() as conn: 
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
async def get_trading_info(
    StockCode: str = Query(..., description='IDX Stock Code (e.g., BBCA, ABBA, GOTO), available options can be viewed through "/api_stock_query_options" endpoint'),
    StartDate: date = Query(..., description='Start Trading Date (Inclusive) in "YYYY-MM-DD" format'),
    EndDate: date = Query(..., description='End Trading Date (Inclusive) in "YYYY-MM-DD" format')
):
    """
        Show daily trading info starting **from 2020**
    """
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    # Filter by StockCode and Date Range
    output_df = pd.read_sql('''
        SELECT * FROM \"IDXTradingInfo\" 
        WHERE
            \"IDXTradingInfo\".\"StockCode\" = \'{}\' 
            AND \"IDXTradingInfo\".\"Date\" >= \'{}\' 
            AND \"IDXTradingInfo\".\"Date\" <= \'{}\'
    '''.format(StockCode, StartDate, EndDate), con=conn)

    return Response(output_df.to_json(orient="records"), media_type="application/json")

@app.get("/financial_reports")
async def get_financial_reports(
    StockCode: str = Query(..., description='IDX Stock Code (e.g., BBCA, ABBA, GOTO), available options can be viewed through "/api_stock_query_options" endpoint'),
    ReportYear: int = Query(..., description='Report Year'),
    ReportPeriod: Optional[str] = Query(None, description='Report Period (TW1, TW2, TW3, Audit), TW is equal to Q / Quarter (TW1=Q1), Audit is Q4')
):
    """
        Show financial reports download URL from the **past 2 years**
    """
    # Your code here to retrieve the company profiles based on the parameters
    # and format the data as a JSON response

    # Filter by StockCode and Date Range
    with engine.connect() as conn:
        if ReportPeriod:
            output_df = pd.read_sql('''
                SELECT * FROM \"IDXFinancialReportLinks\" 
                WHERE 
                    \"IDXFinancialReportLinks\".\"StockCode\" = \'{}\' 
                    AND \"IDXFinancialReportLinks\".\"Report_Period\" = \'{}\' 
                    AND \"IDXFinancialReportLinks\".\"Report_Year\" = \'{}\'
            '''.format(StockCode, ReportPeriod, ReportYear), con=conn)
        else:
            output_df = pd.read_sql('''
                SELECT * FROM \"IDXFinancialReportLinks\" 
                WHERE 
                    \"IDXFinancialReportLinks\".\"StockCode\" = \'{}\' 
                    AND \"IDXFinancialReportLinks\".\"Report_Year\" = \'{}\'
            '''.format(StockCode, ReportYear), con=conn)

    return Response(output_df.to_json(orient="records"), media_type="application/json")