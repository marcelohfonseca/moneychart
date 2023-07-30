#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

import pandas as pd
from investpy import crypto, currency_crosses, etfs, funds, indices, stocks

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.read_config import get_config


def get_assets(dict_settings: dict) -> pd.DataFrame:
    """Faz o download de uma lista de ativos de acordo com as configurações
    listadas no arquivo de configuração do projeto.

    Args:
        dict_settings (dict): dicionário com as configurações de ativos

    Returns:
        pd.DataFrame: dataframe com a listagem distinta dos ativos
    """

    data_folder = dict_settings["data-folder"]
    dict_ignore = dict_settings["ignore"]

    dict_asset_type = {
        "crypto": dict_settings["assets"]["crypto"],
        "currency": dict_settings["assets"]["currency"],
        "etf": dict_settings["assets"]["etf"],
        "funds": dict_settings["assets"]["funds"],
        "indices": dict_settings["assets"]["indices"],
        "stocks": dict_settings["assets"]["stocks"],
    }

    dict_columns = {
        "country": "cd_pais",
        "symbol": "cd_ativo",
        "name": "nm_ativo",
        "full_name": "nm_completo",
        "isin": "cd_isin",
        "currency": "cd_moeda",
        "asset_type": "ds_tipo",
    }
    df_assets = pd.DataFrame(columns=dict_columns.values())

    for asset_type, asset_country in dict_asset_type.items():
        if dict_ignore[asset_type] == False:
            for country in asset_country.keys():
                list_assets = asset_country[country]

                if asset_type == "crypto":
                    df_assets_temp = pd.DataFrame(
                        crypto.get_cryptos(), columns=dict_columns.keys()
                    ).assign(asset_type=asset_type, country=country)

                if asset_type == "currency":
                    df_assets_temp = pd.DataFrame(
                        currency_crosses.get_currency_crosses(),
                        columns=dict_columns.keys(),
                    ).assign(asset_type=asset_type, country=country)

                if asset_type == "etf":
                    df_assets_temp = pd.DataFrame(
                        etfs.get_etfs(country=country), columns=dict_columns.keys()
                    ).assign(asset_type=asset_type)

                if asset_type == "funds":
                    df_assets_temp = pd.DataFrame(
                        funds.get_funds(country=country), columns=dict_columns.keys()
                    ).assign(asset_type=asset_type)

                if asset_type == "indices":
                    df_assets_temp = pd.DataFrame(
                        indices.get_indices(country=country),
                        columns=dict_columns.keys(),
                    ).assign(asset_type=asset_type)

                if asset_type == "stocks":
                    df_assets_temp = pd.DataFrame(
                        stocks.get_stocks(country=country), columns=dict_columns.keys()
                    ).assign(asset_type=asset_type)

                if asset_type == "national_treasury":
                    df_assets_temp = (
                        pd.read_csv("../../setup/national_treasury.csv", sep=";")
                        .query("country == @country")
                        .assign(asset_type=asset_type, cd_isin="")
                    )

                df_assets_temp.query("symbol == @list_assets", inplace=True)
                df_assets_temp.rename(columns=dict_columns, inplace=True)
                df_assets = pd.concat([df_assets, df_assets_temp], ignore_index=True)

    df_assets["nm_completo"].fillna(df_assets["nm_ativo"], inplace=True)
    df_assets["cd_pais"] = df_assets["cd_isin"].str[:2]
    df_assets["cd_emissor"] = df_assets["cd_isin"].str[-10:].str[:4]
    df_assets["cd_tipo_ativo"] = df_assets["cd_isin"].str[-6:].str[:3]
    df_assets["cd_especie"] = df_assets["cd_isin"].str[-3:].str[:2]

    return df_assets


if __name__ == "__main__":
    dict_settings = get_config()

    df_assets = get_assets(dict_settings)
    df_assets.head()
