# Python
import re
import hashlib

# BeautifulSoup
from bs4 import BeautifulSoup

# Pandas
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
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U00002702-\U000027B0"
                                        u"\U000024C2-\U0001F251"
                                        u"\U0001f926-\U0001f937"
                                        u"\U00010000-\U0010ffff"
                                        u"\u2640-\u2642"
                                        u"\u2600-\u2B55"
                                        u"\u200d"
                                        u"\u23cf"
                                        u"\u23e9"
                                        u"\u231a"
                                        u"\ufe0f"  # dingbats
                                        u"\u3030"
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


def extract_text_from_html(column_to_clean: str) -> str:
    """
    This function is used to extract the text from the html.

    :param column_to_clean: column to clean
    :return: column with the text
    """
    if type(column_to_clean) is str:
        text = BeautifulSoup(column_to_clean, features='html.parser').get_text()
        return text
    return 'None'


def generate_column_uid(df: DataFrame, column_name) -> DataFrame:
    """
    This function is used to generate a unique uid with the url_data.

    :param df: dataframe
    :return: dataframe with a new column with the uid
    """
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row[column_name].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    return df
