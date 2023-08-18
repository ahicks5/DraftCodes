import mysql.connector
from SQL_Auth import host, user, password, database
import pandas as pd
import pytz
import time

central = pytz.timezone('America/Chicago')

prod_web_path = '/var/www/html/Website/'

green_shades = {
    1: '#33A036',
    2: '#147917',
    3: '#023020',
    4: '#B59410'
}

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
        ~~INDICATOR START~~
        <div>
            <button id="selectAllBtn">Select All</button>
        </div>
        
        <br>
        
        <div class="table-container">
            <table id="myTable">
        '''

header_row = '<tr><td width="15%">Time</td><td width="36%">Team</td><td width="17%">Spread</td><td width="17%">ML</td><td width="17%">O/U</td></tr>'
space_row_html = '<tr><td colspan="5" style="border:none;"></td></tr>'

table_end = '''
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
            
            // Helper function to safely get an integer attribute
            function getSafeIntAttribute(element, attrName) {
                let val = element.getAttribute(attrName);
                return val ? parseInt(val) : 0;
            }

            ~~INDICATOR END PT 1~~

            function updateCellColors() {
                const table = document.getElementById("myTable");
                const cells = table.getElementsByTagName("td");

                for (let cell of cells) {
                    ~~INDICATOR END PT 2~~

                    if (sum > 0 && greenShades[sum]) {
                        cell.style.backgroundColor = greenShades[sum];
                    } else {
                        cell.style.backgroundColor = '';
                    }
                }
            }

            ~~INDICATOR END PT 3~~
        
        // Select all button code
        // Reference to the selectAll button
        const selectAllBtn = document.getElementById("selectAllBtn");

        // Array of all your checkboxes
        // If your checkboxes have a specific class or are inside a container, adjust the selector accordingly
        const allCheckboxes = document.querySelectorAll("input[type='checkbox']");

        // Event listener for the select all button
        selectAllBtn.addEventListener("click", function() {
            // Determine if we're selecting all or deselecting all based on the first checkbox's state
            const shouldSelectAll = !allCheckboxes[0].checked;
    
            // Iterate over all checkboxes and set their checked property
            allCheckboxes.forEach(checkbox => {
                checkbox.checked = shouldSelectAll;
            });
    
            // Update the button text
            selectAllBtn.innerText = shouldSelectAll ? "Deselect All" : "Select All";
            
            // Call the function to update the cell colors after changing the state of checkboxes
            updateCellColors();
        });
        });
        </script>
'''


class Webpage:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
        )
        self.df = self.get_indicators_from_db()

    def get_indicators_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM mlb_indicators"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def build_sp_cells(self, row):
        spread_col_dict = {
            'better_record': 'better_record',
            'better_profit': 'better_profit',
            'runs_for': 'runs_for',
            'runs_ag': 'runs_ag',
            'runs_diff': 'runs_diff',
            'follow_sharps_sp': 'follow_sharps',
            'follow_public_sp': 'follow_public',
            'fade_sharps_sp': 'fade_sharps',
            'fade_public_sp': 'fade_public',
            'model_prediction_sp': 'model_prediction',
        }

        # away cell
        away_cell = f'<td'
        for col in spread_col_dict:
            away_cell += f' {spread_col_dict[col]}="{int(row[col])}"'
        away_cell += f'>{row["team_1_hcap"]}</td>'

        # home cell
        home_cell = f'<td'
        for col in spread_col_dict:
            home_cell += f' {spread_col_dict[col]}="{int(-row[col])}"'
        home_cell += f'>{row["team_2_hcap"]}</td>'

        return away_cell, home_cell

    def build_ml_cells(self, row):
        ml_col_dict = {
            'better_record': 'better_record',
            'better_profit': 'better_profit',
            'runs_for': 'runs_for',
            'runs_ag': 'runs_ag',
            'runs_diff': 'runs_diff',
            'follow_sharps_ml': 'follow_sharps',
            'follow_public_ml': 'follow_public',
            'fade_sharps_ml': 'fade_sharps',
            'fade_public_ml': 'fade_public',
            'model_prediction_ml': 'model_prediction',
            'espn_prediction': 'espn_prediction'
        }

        # away cell
        away_cell = f'<td'
        for col in ml_col_dict:
            away_cell += f' {ml_col_dict[col]}="{row[col]}"'
        away_cell += f'>{row["team_1_ml_odds"]}</td>'

        # home cell
        home_cell = f'<td'
        for col in ml_col_dict:
            home_cell += f' {ml_col_dict[col]}="{-row[col]}"'
        home_cell += f'>{row["team_2_ml_odds"]}</td>'

        return away_cell, home_cell

    def build_tot_cells(self, row):
        ml_col_dict = {
            'over_under_on_season': 'over_under_on_season',
            'model_prediction_tot': 'model_prediction',
            'follow_sharps_tot': 'follow_sharps',
            'follow_public_tot': 'follow_public',
            'fade_sharps_tot': 'fade_sharps',
            'fade_public_tot': 'fade_public',
        }

        # away cell
        away_cell = f'<td'
        for col in ml_col_dict:
            away_cell += f' {ml_col_dict[col]}="{row[col]}"'
        away_cell += f'>{row["Over_line"]}</td>'

        # home cell
        home_cell = f'<td'
        for col in ml_col_dict:
            home_cell += f' {ml_col_dict[col]}="{-row[col]}"'
        home_cell += f'>{row["Under_line"]}</td>'

        return away_cell, home_cell

    def generate_row(self, row):
        away_sp_cell, home_sp_cell = self.build_sp_cells(row)
        away_ml_cell, home_ml_cell = self.build_ml_cells(row)
        over_cell, under_cell = self.build_tot_cells(row)

        row_html = f'<tr><td>{row["cst_game_date"].strftime("%m/%d/%y")}</td><td>{row["away_team_clean"]}</td>'
        row_html += away_sp_cell + away_ml_cell + over_cell + '</tr>'

        formatted_time = row['cst_game_time']

        row_html += f'<tr><td>{formatted_time} CDT</td><td>{row["home_team_clean"]}</td>'
        row_html += home_sp_cell + home_ml_cell + under_cell + '</tr>' + space_row_html

        return row_html

    def generate_table(self, sport):
        table = table_start
        df = self.df[self.df['bovada_league'] == sport]
        ind_start, ind_end_pt1, ind_end_pt2, ind_end_pt3 = self.generate_script_pieces()

        table += f'<tr><td colspan="5"><b>Upcoming Games</b></td></tr>' + space_row_html
        for i, row in df.iterrows():
            table += header_row
            row_html = self.generate_row(row)
            table += row_html

        table += table_end

        table = table.replace('~~INDICATOR START~~', ind_start)
        table = table.replace('~~INDICATOR END PT 1~~', ind_end_pt1)
        table = table.replace('~~INDICATOR END PT 2~~', ind_end_pt2)
        table = table.replace('~~INDICATOR END PT 3~~', ind_end_pt3)

        return table

    def generate_script_pieces(self):
        indicators = {
            'better_record': ['Better Record', 'betterRecordCheckbox', 'betterRecordInd', 'Favors the team with better win/loss record'],
            'better_profit': ['Better Profit', 'betterProfitCheckbox', 'betterProfitInd', 'Favors the most profitable team if you bet this team Moneyline all season'],
            'runs_for': ['Better Offense', 'runsForCheckbox', 'runsForInd', 'Favors team that averages more runs per game'],
            'runs_ag': ['Better Defense', 'runsAgCheckbox', 'runsAgInd', 'Favors team that averages less runs against per game'],
            'runs_diff': ['Better Point Differential', 'runsDiffCheckbox', 'runsDiffInd', 'Favors team with better differential between runs for and runs against'],
            'over_under_on_season': ['Team Tendencies for Over/Under', 'overUnderOnSeasonCheckbox', 'overUnderOnSeasonInd', 'Using general records of over/unders for both teams, what total is likely to hit'],
            'follow_sharps': ['Follow Sharps', 'followSharpsCheckbox', 'followSharpsInd', 'Follow where most of the betting money is placed'],
            'follow_public': ['Follow Public', 'followPublicCheckbox', 'followPublicInd', 'Follow where most public bets have been placed'],
            'fade_sharps': ['Fade Sharps', 'fadeSharpsCheckbox', 'fadeSharpsInd', 'Go against where most of the betting money is placed'],
            'fade_public': ['Fade Public', 'fadePublicCheckbox', 'fadePublicInd', 'Go against where most public bets have been placed'],
            'model_prediction': ['Predictive Model', 'predictiveModelCheckbox', 'predictiveModelInd', 'Using predictions from statistical models'],
            'espn_prediction': ['ESPN Model', 'espnModelCheckbox', 'espnModelInd', 'Using ESPN statistical models and predictions where available']
        }

        # build indicator start
        ind_start = '<div>'
        for ind in indicators:
            ind_start += f'<div>' \
                         f'<input type="checkbox" id="{ind}">' \
                         f'<label for="{ind}" title="{indicators[ind][3]}">{indicators[ind][0]}</label>' \
                         f'</div>'
        ind_start += '</div>'

        # build indicator end pt 1
        ind_end_pt1 = ''
        for ind in indicators:
            ind_end_pt1 += f'const {indicators[ind][1]} = document.getElementById("{ind}");'


        # build indicator end pt 2
        ind_end_pt2 = ''
        for ind in indicators:
            ind_end_pt2 += f'let {indicators[ind][2]} = getSafeIntAttribute(cell, "{ind}");'
        ind_end_pt2 += '''
                        let sum = 0;
                        '''

        for ind in indicators:
            ind_end_pt2 += f'if ({indicators[ind][1]}.checked) {{' \
                            f'sum += {indicators[ind][2]};}}'
        ind_end_pt2 += '''// If sum is greater than 6, set it to 6
                    if (sum > 6) {
                        sum = 6;
                    }'''

        # build indicator end pt 3
        ind_end_pt3 = ''
        for ind in indicators:
            ind_end_pt3 += f'{indicators[ind][1]}.addEventListener("change", updateCellColors);'

        return ind_start, ind_end_pt1, ind_end_pt2, ind_end_pt3


def generate_mlb_webpage(sport):
    start = time.time()
    mlb = Webpage()
    end = time.time()
    print(f"MLB HTML time taken: {end - start:.2f} seconds")
    return mlb.generate_table(sport)


if __name__ == '__main__':
    mlb = Webpage()
    mlb.generate_table('mlb')
