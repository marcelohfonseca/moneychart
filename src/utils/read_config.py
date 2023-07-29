#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json


def get_config() -> dict:
    """Read the settings.json file and return a dict with the settings of the project

    Returns:
        dict: settings of the project
    """
    try:
        with open("../../setup/settings.json") as json_file:
            config = json.load(json_file)
            return config
    except Exception as err:
        print(f"Error reading the settings.json file: \n{err}")


if __name__ == "__main__":
    print(get_config())
