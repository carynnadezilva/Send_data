# Introduction




## Dataset provided.

You should receive the following files:
- `external-funds` that contains the funds data of instruments that you hold a position to.
- `master-reference-sql.sql` that contains the master reference data of the universe of instruments.


## Case Study

Assume your investment firm has started an investment strategy that invests its capital with 10 funds.

Every month each of the 10 fund sends over their end of month positions. This will include the instrument, price, market value and profit & loss information. The sample funds breakdown data `external-funds` is shared which contains the simulated data for:

- master reference data for bond and equity instruments including reference prices in open db format. You may wish to use any preferred db technology to import this reference data. A suggestion would be in sqlite.
- 10 Funds with 13 months of position change, price & market value data in csv format
- the names of the 10 funds are:

	- Whitestone
	- Wallington
	- Catalysm
	- Belaware
	- Gohen
	- Applebead
	- Magnum
	- Trustmind
	- Leeder
	- Virtous


## TODO
Code up a solution (using Python and SQL) that could potentially be deployed, and describe the gaps/dependencies you will need work on to achieve a production version.

Attempt test-driven development to ensure that your solution is robust.

Factor in the following

1. A sample database with your preferred database (eg. SQLite) which has the reference data setup

2. An ingestion solution to import provided CSV data into created database. Go through the dataset to get familiarized.

3. Use Python and SQL to generate a price reconciliation analysis that shows the difference between `the price on EOM date` from the reference data vs `the price` in the fund report. Programmatically generate the analysis as a CSV file. If the price is unavailable in reference db, use the last available price.

	a. show the break/difference with instrument date ref price vs fund price.
	b. consider how the solution can be scalable for `N` number of fund reports in future.

4. Based on your understanding of the data, use Python and SQL to generate analysis which gives the best performing fund (highest rate of return) for every month. Programmatically generate the analysis as a CSV file.

    You can use the formula 
    Rate of Return = (Fund_MV_end - Fund_MV_start + Realized P/L) / Fund_MV_start
    Fund Market Value = Sum of (Symbol Market Value) for all symbols in that fund.
    Fund Realized P/L = Sum of (Symbol Realized P/L) for the same monthly period.

## Useful Glossary/Tips

1. Position is the amount of a security, asset, or property that is owned (or sold short).

2. Market value is the term used to describe how much an asset or a company is worth on the financial market, according to market participants.

3. A fund is a pool of money set aside for a specific purpose.The pool of money in a fund is often invested and professionally managed in order to generate returns for its investors.

4. An instrument is an implement which is used to store or transfer value or financial obligations. A financial instrument is a tradable or negotiable asset, security, or contract.

5. Buying equity securities, or stocks, means you are buying a very small ownership stake in a company. While bonds/ Debt-based instruments are essentially loans made by an investor to the owner of the asset

6. Reference DB tables :
	a. Bond/Equity Prices - Table with price information for different instruments as of each applicable date
	b. Bond/Equity Reference - Table with all static information related to bond/equity Instruments


## Setup

1. Create venv in location `python3 -m venv venv`
2. Activate `source venv/bin/activate`
3. Install dependencies `pip install -r requirements.txt`

4. Running the script `python investmentfund.py`

## Running tests
1. `pytest tests.py`
2. Output should only be green and passed if all tests past

## Output
1. out/price_reconciliation_analysis.csv
2. out/best_performing_funds.csv

## Scalability and achieving a production ready solution
1. For larger number of funds, we would need to move out of local processing, to a distributed framework to allow for parallel processing across multiple nodes. We would also need to move the data into a proper data warehouse, which can for e.g. partition the data. 
2. Ideally we would also not read the csvs locally but from cloud storage, that way we can also replace the manual running of the scripts with an orchestrator e.g. Airflow, which can manage retries incase of network or connection failures

