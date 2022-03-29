# Shipping emissions API

This processes the EU-MRV shipping emissions reports and exposes the data via a GraphQL API.

# Instructions
- Clone this repo and cd into it
- Create and activate a virtual environment
- Install (`pip install .`)
- Download annual reports from https://mrv.emsa.europa.eu/#public/emission-report to `data/eu-mrv-annual-reports/raw/`
- Run pipeline with `python -m shipping_emissions.pipeline eu-mrv data/eu-mrv-annual-reports/schema.toml data/eu-mrv-annual-reports/raw/*.xlsx` to generate processed data file in `data/db/`
- Start server: `strawberry server shipping_emissions.api.schema`
- Load `http://0.0.0.0:8000/graphql` in a browser
- Use _Explorer_ to construct a query, e.g.
```
query getShip {
  ship(imoNumber: 5383304, reportingPeriod: 2018) {
    name
    reportingPeriod
    annualAverageCo2EmissionsPerDistance
  }
}
```

