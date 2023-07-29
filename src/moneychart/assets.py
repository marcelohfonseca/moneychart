#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

import pandas as pd
from investpy import crypto, currency_crosses, etfs, funds, indices, stocks

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.read_config import get_config

dict_columns = {
    "country": "cd_pais",
    "symbol": "cd_ativo",
    "name": "nm_apelido",
    "full_name": "nm_ativo",
    "isin": "cd_isin",
    "currency": "cd_moeda",
}
df_assets = pd.DataFrame(columns=dict_columns.keys())

dict_settings = get_config()

list_asset_type = {
    "crypto": dict_settings["assets"]["crypto"],
    "currency": dict_settings["assets"]["currency"],
    "etf": dict_settings["assets"]["etf"],
    "funds": dict_settings["assets"]["funds"],
    "indices": dict_settings["assets"]["indices"],
    "stocks": dict_settings["assets"]["stocks"],
}

for asset_type in list_asset_type:
    if asset_type == "crypto":
        df_assets = pd.DataFrame(
            crypto.get_cryptos(), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "currency":
        df_assets = pd.DataFrame(
            currency_crosses.get_currency_crosses(), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "etf":
        df_assets = pd.DataFrame(
            etfs.get_etfs(country="brazil"), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "funds":
        df_assets = pd.DataFrame(
            funds.get_funds(country="brazil"), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "indices":
        df_assets = pd.DataFrame(
            indices.get_indices(country="brazil"), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "stocks":
        df_assets = pd.DataFrame(
            stocks.get_stocks(country="brazil"), columns=dict_columns.keys()
        ).assign(asset_type=asset_type)

    if asset_type == "national_treasury":
        df_assets = pd.read_csv("../../setup/national_treasury.csv", sep=";").assign(
            asset_type=asset_type
        )

print(df_assets.query("asset_type == 'national_treasury'"))
