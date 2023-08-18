from mlb_indicators import run_and_upload_mlb_indicators
from build_mlb_html import generate_mlb_webpage
import datetime
import pytz


webpage_dict = {
    'mlb': ['mlb', 'MLB Baseball', 'baseball-mlb'],
    'nfl': ['nfl', 'NFL Football', 'football-nfl'],
    'cfb': ['college-football', 'NCAA Football', 'football-college']
}


def update_mlb_page():
    run_and_upload_mlb_indicators()

    for sport in webpage_dict:
        table_html = generate_mlb_webpage(webpage_dict[sport][0])

        template_filepath = r'/var/www/html/Website/' + 'Templates/sport_general.html'
        with open(template_filepath, 'r') as f:
            html_content = f.read()

        modified_html = html_content.replace('~Place Table Here~', table_html)
        modified_html = modified_html.replace('{sport_header}', f'{webpage_dict[sport][1]}')

        current_datetime = datetime.datetime.now(pytz.timezone('US/Central'))
        current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p") + ' CDT'

        modified_html = modified_html.replace('Last Refreshed:', current_datetime_str)

        final_html = r'/var/www/html/Website/' + f'{webpage_dict[sport][2]}.html'
        with open(final_html, 'w') as f:
            f.write(modified_html)

        print(f'{sport} page updated...')


if __name__ == '__main__':
    update_mlb_page()
