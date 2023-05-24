from bovadaAPI import PullBovada
from Indicators import Indicators
import datetime
import pytz


class HtmlTable:
    def __init__(self):
        ind = Indicators()
        self.df = ind.sharp_indicator()

    def generate_table(self):
        table = '''
        <style>
            .table-container {
                width: 90%;
                justify-content: center;
                margin-left: auto;
                margin-right: auto;
            }
            table {
                border-collapse: collapse;
                width: 100%;  /* Adjust the width as per your requirement */
            }
            th, td {
                border: 1px solid black;
                padding: 10px;
                text-align: center;
                font-size: 14px; /* Adjust the font size as needed */
            }
            @media screen and (max-width: 600px) {
                th, td {
                    font-size: 12px; /* Reduce font size on smaller screens */
                }
            }
        </style>
        <div class="table-container">
            <table>
        '''

        header_row = '<tr><td width="15%">Game Time</td><td width="36%">Team</td><td width="17%">Spread</td><td width="17%">ML</td><td width="17%">O/U</td></tr>'

        sports_dict = {sport: self.df[self.df['game_sport'] == sport] for sport in self.df['game_sport'].unique()}
        space_row_html = '<tr><td colspan="5" style="border:none;"></td></tr>'

        for sport, df in sports_dict.items():
            table += f'<tr><td colspan="5"><b>{sport}</b></td></tr>' + space_row_html
            for i, row in df.iterrows():
                table += header_row
                row_html = self.generate_row(row)
                table += row_html

        table += '</table></div>'

        return table

    def generate_row(self, row):
        space_row_html = '<tr><td colspan="5" style="border:none;"></td></tr>'

        away_sp_cell, home_sp_cell = self.spread_sharp_tags(row['sharp_spread_ind'], row['team_1_hcap'], row['team_2_hcap'])
        away_ml_cell, home_ml_cell = self.ml_sharp_tags(row['sharp_moneyline_ind'], row['team_1_ml_odds'],
                                                            row['team_2_ml_odds'])
        over_cell, under_cell = self.total_sharp_tags(row['sharp_total_ind'], row['total_over'],
                                                            row['total_under'])

        row_html = f'<tr><td>{row["game_date"].strftime("%m/%d/%y")}</td><td>{row["competitor_1"]}</td>'
        row_html += away_sp_cell + away_ml_cell + over_cell + '</tr>'

        row_html += f'<tr><td>{row["game_time"]}</td><td>{row["competitor_2"]}</td>'
        row_html += home_sp_cell + home_ml_cell + under_cell + '</tr>' + space_row_html

        return row_html

    def spread_sharp_tags(self, indicator, value_1, value_2):
        if indicator == 'Away':
            away_sp_cell = f'<td style="background-color: #32CD32">{value_1}</td>'
            home_sp_cell = f'<td>{value_2}</td>'
        elif indicator == 'Home':
            away_sp_cell = f'<td>{value_1}</td>'
            home_sp_cell = f'<td style="background-color: #32CD32">{value_2}</td>'
        else:
            away_sp_cell = f'<td>{value_1}</td>'
            home_sp_cell = f'<td>{value_2}</td>'

        return away_sp_cell, home_sp_cell

    def ml_sharp_tags(self, indicator, value_1, value_2):
        if indicator == 'Away':
            away_ml_cell = f'<td style="background-color: #32CD32">{value_1}</td>'
            home_ml_cell = f'<td>{value_2}</td>'
        elif indicator == 'Home':
            away_ml_cell = f'<td>{value_1}</td>'
            home_ml_cell = f'<td style="background-color: #32CD32">{value_2}</td>'
        else:
            away_ml_cell = f'<td>{value_1}</td>'
            home_ml_cell = f'<td>{value_2}</td>'

        return away_ml_cell, home_ml_cell

    def total_sharp_tags(self, indicator, value_1, value_2):
        if indicator == 'Over':
            over_cell = f'<td style="background-color: #32CD32">{value_1}</td>'
            under_cell = f'<td>{value_2}</td>'
        elif indicator == 'Under':
            over_cell = f'<td>{value_1}</td>'
            under_cell = f'<td style="background-color: #32CD32">{value_2}</td>'
        else:
            over_cell = f'<td>{value_1}</td>'
            under_cell = f'<td>{value_2}</td>'

        return over_cell, under_cell

    def replace_production(self):
        table = self.generate_table()

        template_html = '/var/www/html/Templates/subscribetemplate.html'
        identifier = '{Upcoming Sports}'
        final_html = '/var/www/html/subscribe.html'

        with open(template_html, 'r') as f:
            html_content = f.read()

        modified_html = html_content.replace(identifier, table)

        current_datetime = datetime.datetime.now(pytz.timezone('US/Central'))
        current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p") + ' CST'

        modified_html = modified_html.replace('{This is a placeholder}', current_datetime_str)

        # add time

        with open(final_html, 'w') as f:
            f.write(modified_html)

        print('Python script ran and is done!')

    def replace_local(self):
        table = self.generate_table()

        template_html = 'practice_template.html'
        identifier = '{Upcoming Sports}'
        final_html = 'practice_main.html'

        with open(template_html, 'r') as f:
            html_content = f.read()

        modified_html = html_content.replace(identifier, table)

        current_datetime = datetime.datetime.now()
        current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p")

        modified_html = modified_html.replace('{This is a placeholder}', current_datetime_str)

        # add time

        with open(final_html, 'w') as f:
            f.write(modified_html)

        print('Python script ran and is done!')


if __name__ == '__main__':
    html = HtmlTable()
    html.replace_production()