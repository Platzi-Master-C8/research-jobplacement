# Python
import re
import hashlib

# BeautifulSoup
from bs4 import BeautifulSoup
from pandas import DataFrame


def remove_emojis(text: str) -> str:
    """
    This function is used to remove emojis from the text.

    :param text: text to clean
    :return: text without emojis
    """
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


def extract_text_from_html(column_to_clean: str) -> str:
    """
    This function is used to extract the text from the html.

    :param column_to_clean: column to clean
    :return: column with the text
    """
    text = BeautifulSoup(column_to_clean).get_text()
    return text


def generate_column_uid(df: DataFrame) -> DataFrame:
    """
    This function is used to generate a unique uid with the url_data.

    :param df: dataframe
    :return: dataframe with a new column with the uid
    """
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url_data'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    df.set_index('uid', inplace=True)
    return df
