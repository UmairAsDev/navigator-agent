import os
import requests
import pandas as pd



token = os.getenv("ACCESS_TOKEN_GOV")
baseUrl = 'https://datawebws.usitc.gov/dataweb'
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": f"Bearer {token}"
}


def countries_list():
    resp = requests.get(baseUrl + "/api/v2/country/getAllCountries", headers=headers, verify=False)
    data = resp.json()
    countries = data.get('options', [])
    df = pd.DataFrame(countries)
    df = df.rename(columns={
        "name": "country_name",
        "iso2": "iso_2_code",
        "iso3": "iso_3_code"
    })

    df = df[["country_name", "iso_2_code", "iso_3_code"]]
    return df
