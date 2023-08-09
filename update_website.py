from mlb_indicators import run_and_upload_mlb_indicators
from build_mlb_html import generate_mlb_webpage
import datetime
import pytz


def update_mlb_page():
    run_and_upload_mlb_indicators()
    table_html = generate_mlb_webpage()

    template_filepath = r'/var/www/html/Website/' + 'Templates/sport_general.html'
    with open(template_filepath, 'r') as f:
        html_content = f.read()

    modified_html = html_content.replace('~Place Table Here~', table_html)
    modified_html = modified_html.replace('{sport_header}', 'Baseball')

    current_datetime = datetime.datetime.now(pytz.timezone('US/Central'))
    current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p") + ' CDT'

    modified_html = modified_html.replace('Last Refreshed:', current_datetime_str)

    final_html = r'/var/www/html/Website/' + 'baseball.html'
    with open(final_html, 'w') as f:
        f.write(modified_html)


if __name__ == '__main__':
    update_mlb_page()