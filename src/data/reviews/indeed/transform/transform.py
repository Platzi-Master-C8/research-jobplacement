# Python
from datetime import datetime

# Utils
from utils.files import save_data, read_data
from bs4 import BeautifulSoup


def transform():
    """
    Transform Indeed data to a more usable format for the model.
    """
    df = read_data('indeed_companies_reviews.csv', 'raw')
    df['user_info'] = df['user_info'].apply(get_text_from_html)

    df['position_user'] = df['user_info'].apply(lambda position_user: position_user[0].split('(')[0])
    df['is_still_working_here'] = df['user_info'].apply(lambda employee: 'Current Employee' in employee[0])
    df['job_location'] = df['user_info'].apply(lambda job_location: job_location[1])
    df['review_date'] = df['user_info'].apply(get_review_date)
    df.drop(columns=['user_info'], inplace=True)
    save_data(df, 'indeed_companies_reviews.csv', 'processed')


def get_text_from_html(data):
    """
    Get text from html. This is used to get the position and the employee status.
    """
    soup = BeautifulSoup(data, 'html.parser')
    return soup.get_text().split('-')


def get_review_date(data):
    """
    Get the review date. This is used to get the review date.
    """
    try:
        return datetime.strptime(data[2], ' %B %d, %Y').strftime('%Y-%m-%d')
    except ValueError:
        return None
