import pandas as pd
import pytest
from investmentfund import InvestmentFunds

analytics = InvestmentFunds(db_name=":memory:")


def test_fund_name_extraction():

    filename = "2022-12-31_Whitestone_Position.csv"
    fund, _ = analytics.parse_filename(filename)
    assert fund == "Whitestone"


def test_date_parsing_standardization():

    filename = "Belaware_31_08_2022.csv"
    _, report_date = analytics.parse_filename(filename)
    assert report_date == "8/31/2022"
