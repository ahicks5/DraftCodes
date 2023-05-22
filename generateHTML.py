from bovadaAPI import PullBovada
from Indicators import Indicators
import datetime


def generate_table(template_html, identifier, final_html):
    ind = Indicators()
    df = ind.sharp_indicator()

    cols = ['game_sport', 'competitor_1', 'competitor_2', 'team_1_hcap', 'team_2_hcap', 'team_1_ml_odds', 'team_2_ml_odds', 'Spread_Ind']

    df_short = df[cols]

    # only baseball for now
    #df_short = df_short[df_short['game_sport'] == 'SOCC']

    table = '''
    <style>
        .table-container {
            display: flex;
            justify-content: center;
        }
        table {
            border-collapse: collapse;
            width: 75%;
        }
        th, td {
            border: 1px solid black;
            padding: 10px;
            text-align: center;
            font-size: 14px; /* Adjust the font size as needed */
        }
    </style>
    <div class="table-container">
        <table>
            <tr><td>Sport</td><td>Team Name</td><td>Handicap</td><td>Moneyline</td></tr>
    '''

    for i, row in df_short.iterrows():
        if row['Spread_Ind'] == 'Away':
            away_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_1"]}</td><td style="background-color: #32CD32">{row["team_1_hcap"]}</td><td>{row["team_1_ml_odds"]}</td></tr>'
            home_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_2"]}</td><td>{row["team_2_hcap"]}</td><td>{row["team_2_ml_odds"]}</td></tr>'
        elif row['Spread_Ind'] == 'Home':
            away_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_1"]}</td><td>{row["team_1_hcap"]}</td><td>{row["team_1_ml_odds"]}</td></tr>'
            home_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_2"]}</td><td style="background-color: #32CD32">{row["team_2_hcap"]}</td><td>{row["team_2_ml_odds"]}</td></tr>'
        else:
            away_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_1"]}</td><td>{row["team_1_hcap"]}</td><td>{row["team_1_ml_odds"]}</td></tr>'
            home_team_row_html = f'<tr><td>{row["game_sport"]}</td><td>{row["competitor_2"]}</td><td>{row["team_2_hcap"]}</td><td>{row["team_2_ml_odds"]}</td></tr>'

        # Add space row
        space_row_html = '<tr><td colspan="4" style="border:none;"></td></tr>'

        table += away_team_row_html + home_team_row_html + space_row_html

    table += '</table></div>'

    with open(template_html, 'r') as f:
        html_content = f.read()

    modified_html = html_content.replace(identifier, table)

    current_datetime = datetime.datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%s")

    modified_html = modified_html.replace('{This is a placeholder}', current_datetime_str)

    # add time

    with open(final_html, 'w') as f:
        f.write(modified_html)

    print('Python script ran and is done!')


if __name__ == '__main__':
    template_html = '/var/www/html/Templates/subscribetemplate.html'
    identifier = '{Upcoming Sports}'
    final_html = '/var/www/html/subscribe.html'

    generate_table(template_html, identifier, final_html)