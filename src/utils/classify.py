#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
from urllib.request import urlopen
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests as r
from bs4 import BeautifulSoup as bs
from lxml import html
from tqdm import tqdm

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
}


def get_b3_classification() -> pd.DataFrame:
    """Search the rankings of companies listed on B3. These data are provided
    in an unstructured Excel spreadsheet, therefore it is necessary to download,
    process and fill in the empty lines.

    Returns:
        pd.DataFrame: A dataframe with the new information.
    """
    URL = "http://www.b3.com.br/lumis/portal/file/fileDownload.jsp?fileId="
    FILE = "8AA8D0975A2D7918015A3C81693D4CA4"

    file = ZipFile(io.BytesIO(urlopen(URL + FILE).read()))

    df_b3 = pd.read_excel(file.open(file.filelist[0]))
    df_b3 = df_b3[df_b3.columns[:4]]
    df_b3.columns = ["nm_setor", "nm_subsetor", "nm_segmento", "cd_emissor"]

    df_b3["nm_setor"] = df_b3["nm_setor"].str.replace(", ", " ")
    df_b3["nm_subsetor"] = df_b3["nm_subsetor"].str.replace(", ", " ")
    df_b3["nm_segmento"] = df_b3["nm_segmento"].str.replace(", ", " ")
    df_b3["nm_classe"] = "Ações"

    df_b3["nm_segmento"] = np.where(
        df_b3["cd_emissor"].notna(), np.nan, df_b3["nm_segmento"]
    )
    df_b3["drop"] = np.where(df_b3["cd_emissor"].notna(), False, True)
    df_b3 = df_b3.ffill(axis=0)
    df_b3 = df_b3.loc[
        (df_b3["nm_setor"] != "SETOR ECONÔMICO")
        & (df_b3["cd_emissor"] != "CÓDIGO")
        & (df_b3["drop"] == False)
    ]
    df_b3 = df_b3.drop(columns="drop")

    return df_b3


def get_fiis_classification() -> pd.DataFrame:
    """Search for classifications of real estate funds provided by fiis.com.
    For this, a scraping must be done for each FII.

    Returns:
        pd.DataFrame: A dataframe with the new information.
    """
    URL = "https://fiis.com.br/lista-de-fundos-imobiliarios/"

    page = r.get(URL, headers=headers)
    content = bs(page.content, "html.parser")
    response = content.find_all("div", attrs={"data-element": "ticker-box-title"})
    list_fiis = [titulo_tag.text for titulo_tag in response]
    list_error = []

    df_fiis = pd.DataFrame(
        columns=["cd_ativo", "nm_classe", "nm_setor", "nm_subsetor", "nm_segmento"]
    )

    for fii in tqdm(list_fiis[:5]):
        try:
            url = f"https://fiis.com.br/{fii}"
            page = r.get(url, headers=headers)
            content = html.fromstring(page.content)

            type_fii = content.xpath(
                '//*[@id="carbon_fields_fiis_informations-2"]/div[3]/p[5]/b'
            )
            segment_fii = content.xpath(
                '//*[@id="carbon_fields_fiis_informations-2"]/div[3]/p[6]/b'
            )

            df_fiis_temp = pd.DataFrame(
                data={
                    "cd_ativo": [fii],
                    "ds_tipo": [type_fii[0].text],
                    "nm_segmento": [segment_fii[0].text],
                }
            )
            df_fiis_temp["nm_setor"] = df_fiis_temp["ds_tipo"].str.split(":").str[0]
            df_fiis_temp["nm_subsetor"] = df_fiis_temp["ds_tipo"].str.split(":").str[1]
            df_fiis_temp["nm_classe"] = "Fundos Imobiliários"
            df_fiis_temp.drop(columns="ds_tipo", inplace=True)
            df_fiis = pd.concat([df_fiis, df_fiis_temp])
        except:
            list_error.append(fii)

    return df_fiis
