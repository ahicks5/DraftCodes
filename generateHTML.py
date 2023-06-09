from bovadaAPI import PullBovada
from Indicators import Indicators
import datetime
import pytz
import pandas as pd
from connectSources import find_ref_dfs
from clean_files import CleanFiles

team_ref_df, sport_ref_df, espn_schedule_df, bovada_df = find_ref_dfs()
central = pytz.timezone('America/Chicago')

class HtmlTable:
    prod_web_path = '/var/www/html/Website/'

    green_shades = {
        1: '#33A036',
        2: '#147917',
        3: '#023020',
        4: '#B59410'
    }

    #legend_table = '''
    #    '''

    table_start = '''
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
          input[type="checkbox"]:checked + span {
            background-color: blue;
          }
        </style>
        <input type="checkbox" id="sharp_checkbox"><label for="sharp_checkbox">Sharps</label>
        <input type="checkbox" id="espn_pred_checkbox"><label for="espn_pred_checkbox">ESPN</label>
        <input type="checkbox" id="espn_streak_checkbox"><label for="espn_streak_checkbox">Streaks</label>
        <input type="checkbox" id="espn_avg_checkbox"><label for="espn_avg_checkbox">Averages</label>
        <div class="table-container">
            <table id="myTable">
        '''
    header_row = '<tr><td width="15%">Time</td><td width="36%">Team</td><td width="17%">Spread</td><td width="17%">ML</td><td width="17%">O/U</td></tr>'
    space_row_html = '<tr><td colspan="5" style="border:none;"></td></tr>'

    def __init__(self):
        ind = Indicators()
        self.df = ind.sharp_indicator()
        self.df['game_date'] = pd.to_datetime(self.df['game_date'])
        self.df['game_startTime_cst'] = pd.to_datetime(self.df['game_startTime_cst'])
        self.clean = CleanFiles()
        try:
            self.df.to_csv('/var/www/html/Website/Indicator_Data.csv', index=False)
        except:
            self.df.to_csv('ESPN_Data.csv', index=False)

    def generate_tables(self):
        fin_dict = {}
        sports_dict = {sport: self.df[self.df['Clean_Sport'] == sport] for sport in self.df['Clean_Sport'].unique()}

        for sport, df in sports_dict.items():
            table = self.table_start
            header_row = self.header_row
            space_row_html = self.space_row_html

            table += f'<tr><td colspan="5"><b>{sport}</b></td></tr>' + space_row_html
            for i, row in df.iterrows():
                table += header_row
                row_html = self.generate_row(row)
                table += row_html

            table += '''
                </table></div>
                    <script>
                        document.addEventListener("DOMContentLoaded", function () {
                        const greenShades = {
                            1: '#2D9CDB',
                            2: '#6FCF97',
                            3: '#F2C94C',
                            4: '#FF7E67',
                            5: '#EB5757',
                            6: '#B4161B',                            
                        };
                    
                        const sharpCheckbox = document.getElementById("sharp_checkbox");
                        const espnPredCheckbox = document.getElementById("espn_pred_checkbox");
                        const espnStreakIndCheckbox = document.getElementById("espn_streak_checkbox");
                        const espnAvgIndCheckbox = document.getElementById("espn_avg_checkbox");
                    
                        function updateCellColors() {
                            const table = document.getElementById("myTable");
                            const cells = table.getElementsByTagName("td");
                    
                            for (let cell of cells) {
                                let sharpInd = parseInt(cell.getAttribute("sharp_ind"));
                                let espnPredInd = parseInt(cell.getAttribute("espn_pred_ind"));
                                let espnStreakInd = parseInt(cell.getAttribute("espn_stk_ind"));
                                let espnAvgInd = parseInt(cell.getAttribute("espn_avg_ind"));
                                let sum = 0;
                    
                                if (sharpCheckbox.checked) {
                                    sum += sharpInd;
                                }
                                if (espnPredCheckbox.checked) {
                                    sum += espnPredInd;
                                }
                                if (espnStreakIndCheckbox.checked) {
                                    sum += espnStreakInd;
                                }
                                
                                if (espnAvgIndCheckbox.checked) {
                                    sum += espnAvgInd;
                                }
                                
                                // If sum is greater than 6, set it to 6
                                if (sum > 6) {
                                    sum = 6;
                                }
                    
                                if (sum > 0 && greenShades[sum]) {
                                    cell.style.backgroundColor = greenShades[sum];
                                } else {
                                    cell.style.backgroundColor = '';
                                }
                            }
                        }
                    
                        sharpCheckbox.addEventListener("change", updateCellColors);
                        espnPredCheckbox.addEventListener("change", updateCellColors);
                        espnStreakIndCheckbox.addEventListener("change", updateCellColors);
                        espnAvgIndCheckbox.addEventListener("change", updateCellColors);
                    });
                    </script>
            '''

            fin_dict[sport] = table

        return fin_dict

    def generate_table(self):
        table = self.table_start

        header_row = self.header_row

        sports_dict = {sport: self.df[self.df['Clean_Sport'] == sport] for sport in self.df['Clean_Sport'].unique()}
        space_row_html = self.space_row_html

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

        away_sp_cell, home_sp_cell = self.spread_sharp_tags(row['sharp_spread_ind'], row['espn_streak_ind'], row['espn_avg_sp_ind'], row['team_1_hcap'], row['team_2_hcap'])
        away_ml_cell, home_ml_cell = self.ml_sharp_tags(row['sharp_moneyline_ind'], row['espn_moneyline_ind'],  row['espn_streak_ind'], row['espn_avg_ml_ind'], row['team_1_ml_odds'],
                                                            row['team_2_ml_odds'])
        over_cell, under_cell = self.total_sharp_tags(row['sharp_total_ind'], row['espn_avg_ou_ind'], row['total_over'],
                                                            row['total_under'])

        row_html = f'<tr><td>{row["game_date"].strftime("%m/%d/%y")}</td><td>{row["competitor_2"]}</td>'
        row_html += away_sp_cell + away_ml_cell + over_cell + '</tr>'

        localized_datetime = central.normalize(row['game_startTime_cst'].astimezone(central))
        formatted_time = localized_datetime.strftime("%I:%M %p %Z")

        row_html += f'<tr><td>{formatted_time}</td><td>{row["competitor_1"]}</td>'
        row_html += home_sp_cell + home_ml_cell + under_cell + '</tr>' + space_row_html

        return row_html

    def spread_sharp_tags(self, indicator, espn_streak, espn_avg_spread, value_1, value_2):
        if (pd.isna(value_1)) or (pd.isna(value_2)):
            indicator = 0
            value_1 = value_2 = '-'

        if indicator > 0:
            away_sp_cell = f'<td sharp_ind="{indicator}" espn_pred_ind="0" espn_stk_ind="{espn_streak}" espn_avg_ind="{espn_avg_spread}">{value_1}</td>'
            home_sp_cell = f'<td sharp_ind="{-indicator}" espn_pred_ind="0" espn_stk_ind="{-espn_streak}" espn_avg_ind="{-espn_avg_spread}">{value_2}</td>'
        elif indicator < 0:
            indicator, espn_streak, espn_avg_spread = abs(indicator), abs(espn_streak), abs(espn_avg_spread)
            away_sp_cell = f'<td sharp_ind="{-indicator}" espn_pred_ind="0" espn_stk_ind="{-espn_streak}" espn_avg_ind="{-espn_avg_spread}">{value_1}</td>'
            home_sp_cell = f'<td sharp_ind="{indicator}" espn_pred_ind="0" espn_stk_ind="{espn_streak}" espn_avg_ind="{espn_avg_spread}">{value_2}</td>'
        else:
            away_sp_cell = f'<td>{value_1}</td>'
            home_sp_cell = f'<td>{value_2}</td>'

        return away_sp_cell, home_sp_cell

    def ml_sharp_tags(self, sharp_indicator, espn_pred, espn_streak, espn_avg_ml, value_1, value_2):
        if (pd.isna(value_1)) or (pd.isna(value_2)):
            sharp_indicator = 0
            value_1 = value_2 = '-'

        if sharp_indicator > 0:
            away_ml_cell = f'<td sharp_ind="{sharp_indicator}" espn_pred_ind="{espn_pred}" espn_stk_ind="{espn_streak}" espn_avg_ind="{espn_avg_ml}">{value_1}</td>'
            home_ml_cell = f'<td sharp_ind="{-sharp_indicator}" espn_pred_ind="{-espn_pred}" espn_stk_ind="{-espn_streak}" espn_avg_ind="{-espn_avg_ml}">{value_2}</td>'
        elif sharp_indicator < 0:
            sharp_indicator, espn_pred, espn_streak, espn_avg_ml = abs(sharp_indicator), abs(espn_pred), abs(espn_streak), abs(espn_avg_ml)
            away_ml_cell = f'<td sharp_ind="{-sharp_indicator}" espn_pred_ind="{-espn_pred}" espn_stk_ind="{-espn_streak}" espn_avg_ind="{-espn_avg_ml}">{value_1}</td>'
            home_ml_cell = f'<td sharp_ind="{sharp_indicator}" espn_pred_ind="{espn_pred}" espn_stk_ind="{espn_streak}" espn_avg_ind="{espn_avg_ml}">{value_2}</td>'
        else:
            away_ml_cell = f'<td>{value_1}</td>'
            home_ml_cell = f'<td>{value_2}</td>'

        return away_ml_cell, home_ml_cell

    def total_sharp_tags(self, indicator, espn_avg_tot, value_1, value_2):
        if (pd.isna(value_1)) or (pd.isna(value_2)):
            indicator = 0
            value_1 = value_2 = '-'

        if indicator > 0:
            over_cell = f'<td sharp_ind="{indicator}" espn_pred_ind="0" espn_stk_ind="0" espn_avg_ind="{espn_avg_tot}">{value_1}</td>'
            under_cell = f'<td sharp_ind="{-indicator}" espn_pred_ind="0" espn_stk_ind="0" espn_avg_ind="{-espn_avg_tot}">{value_2}</td>'
        elif indicator < 0:
            indicator, espn_avg_tot = abs(indicator), abs(espn_avg_tot)
            over_cell = f'<td sharp_ind="{-indicator}" espn_pred_ind="0" espn_stk_ind="0"  espn_avg_ind="{-espn_avg_tot}">{value_1}</td>'
            under_cell = f'<td sharp_ind="{indicator}" espn_pred_ind="0" espn_stk_ind="0" espn_avg_ind="{espn_avg_tot}">{value_2}</td>'
        else:
            over_cell = f'<td>{value_1}</td>'
            under_cell = f'<td>{value_2}</td>'

        return over_cell, under_cell

    def replace_prod_pages(self):
        fin_dict = self.generate_tables()
        page_dict = dict(zip(sport_ref_df['Clean_Sport'], sport_ref_df['Page_name']))

        for sport in page_dict:
            if sport not in fin_dict:
                template_html = self.prod_web_path + 'Templates/sport_general.html'
                with open(template_html, 'r') as f:
                    html_content = f.read()

                modified_html = html_content.replace('~Place Table Here~', 'No Games Available')

                # replace sport header
                modified_html = modified_html.replace('{sport_header}', sport)

                current_datetime = datetime.datetime.now(pytz.timezone('US/Central'))
                current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p") + ' CDT'

                modified_html = modified_html.replace('Last Refreshed:', current_datetime_str)

                # add time

                final_html = self.prod_web_path + page_dict[sport]
                with open(final_html, 'w') as f:
                    f.write(modified_html)
            else:
                template_html = self.prod_web_path + 'Templates/sport_general.html'
                with open(template_html, 'r') as f:
                    html_content = f.read()

                modified_html = html_content.replace('~Place Table Here~', fin_dict[sport])

                # replace sport header
                modified_html = modified_html.replace('{sport_header}', sport)

                current_datetime = datetime.datetime.now(pytz.timezone('US/Central'))
                current_datetime_str = 'Last Refreshed: ' + current_datetime.strftime("%m/%d/%Y %I:%M %p") + ' CDT'

                modified_html = modified_html.replace('Last Refreshed:', current_datetime_str)

                # add time

                final_html = self.prod_web_path + page_dict[sport]
                with open(final_html, 'w') as f:
                    f.write(modified_html)

        #self.clean.clean_all()
        print('Python has updated all pages!')


if __name__ == '__main__':
    html = HtmlTable()
    html.replace_prod_pages()