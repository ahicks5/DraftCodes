from bovadaAPI import PullBovada
from Indicators import Indicators


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
            width: 50%;
        }
        th, td {
            border: 1px solid black;
            padding: 10px;
            text-align: center;
        }
        th {
            background-color: #D3D3D3;
        }
        tr:nth-child(3n){
            background-color: #f1f1f1;
        }
    </style>
    <div class="table-container">
        <table>
            <tr><th>Sport</th><th>Team Name</th><th>Handicap</th><th>Moneyline</th></tr>
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

    with open(final_html, 'w') as f:
        f.write(modified_html)

    print('Python script ran and is done!')


if __name__ == '__main__':
    template_html = 'practice_template.html'
    identifier = '{identifier}'
    final_html = 'practice_main.html'

    generate_table(template_html, identifier, final_html)