import pandas as pd
from SQL_Auth import host, user, password, database
from sqlalchemy import create_engine


def post_updated_mlb_indicators(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='mlb_indicators', con=engine, if_exists='replace', index=False)


def apply_all_indicators(df):
    ind_dict = {
        'better_record': better_record,
        'better_profit': better_profit,
        'over_under_on_season': over_under_on_season,
        'runs_for': better_runs_for,
        'runs_ag': better_runs_ag,
        'runs_diff': better_runs_diff,
        'model_prediction_ml': vsin_model_prediction_ml,
        'model_prediction_sp': vsin_model_prediction_spread,
        'model_prediction_tot': vsin_model_prediction_total,
        'follow_sharps_ml': follow_sharps_ml,
        'follow_sharps_sp': follow_sharps_spread,
        'follow_sharps_tot': follow_sharps_total,
        'follow_public_ml': follow_public_ml,
        'follow_public_sp': follow_public_spread,
        'follow_public_tot': follow_public_total,
        'fade_sharps_ml': fade_sharps_ml,
        'fade_sharps_sp': fade_sharps_spread,
        'fade_sharps_tot': fade_sharps_total,
        'fade_public_ml': fade_public_ml,
        'fade_public_sp': fade_public_spread,
        'fade_public_tot': fade_public_total,
    }

    for ind in ind_dict:
        df[ind] = df.apply(ind_dict[ind], axis=1)

    df = df[['DC_Game_ID'] + list(ind_dict.keys())]
    df = df.fillna(0)

    return df


# check all data is there
def check_data(row, column_names):
    for col in column_names:
        if pd.isna(row[col]):
            return False

    return True


# Better Record
def better_record(row):
    if check_data(row, ['away_record', 'home_record']):
        away_wins = row['away_record'].split('-')[0]
        away_losses = row['away_record'].split('-')[1]
        away_win_percent = int(away_wins) / (int(away_wins) + int(away_losses))

        home_wins = row['home_record'].split('-')[0]
        home_losses = row['home_record'].split('-')[1]
        home_win_percent = int(home_wins) / (int(home_wins) + int(home_losses))

        if away_win_percent > home_win_percent:
            return 1
        elif home_win_percent > away_win_percent:
            return -1
        else:
            return 0
    else:
        return 0


# better profit
def better_profit(row):
    if check_data(row, ['away_profit', 'home_profit']):
        away_profit = int(row['away_profit'][1:].replace(",", '').replace('$', '').strip())
        home_profit = int(row['home_profit'][1:].replace(",", '').replace('$', '').strip())
        if away_profit > home_profit:
            return 1
        elif home_profit > away_profit:
            return -1
        else:
            return 0
    else:
        return 0


# over/under seasonal tendency
def over_under_on_season(row):
    if check_data(row, ['away_o-u-p', 'home_o-u-p']):
        total_overs = int(row['away_o-u-p'].split('-')[0]) + int(row['home_o-u-p'].split('-')[0])
        total_unders = int(row['away_o-u-p'].split('-')[1]) + int(row['home_o-u-p'].split('-')[1])
        if total_overs > total_unders:
            return 1
        elif total_unders > total_overs:
            return -1
        else:
            return 0
    else:
        return 0


def better_runs_for(row):
    if check_data(row, ['away_rf', 'home_rf']):
        away_rf = float(row['away_rf'])
        home_rf = float(row['home_rf'])
        if away_rf > home_rf:
            return 1
        elif home_rf > away_rf:
            return -1
        else:
            return 0
    else:
        return 0


def better_runs_ag(row):
    if check_data(row, ['away_ra', 'home_ra']):
        away_ra = float(row['away_ra'])
        home_ra = float(row['home_ra'])
        if away_ra < home_ra:
            return 1
        elif home_ra < away_ra:
            return -1
        else:
            return 0
    else:
        return 0


def better_runs_diff(row):
    if check_data(row, ['away_ra', 'home_ra', 'away_rf', 'home_rf']):
        away_ra = float(row['away_ra'])
        home_ra = float(row['home_ra'])
        away_rf = float(row['away_rf'])
        home_rf = float(row['home_rf'])
        away_run_diff = away_rf - away_ra
        home_run_diff = home_rf - home_ra

        if away_run_diff < home_run_diff:
            return 1
        elif home_run_diff < away_run_diff:
            return -1
        else:
            return 0


def vsin_model_prediction_ml(row):
    if check_data(row, ['away_est_score', 'home_est_score']):
        away_score = float(row['away_est_score'])
        home_score = float(row['home_est_score'])
        if away_score < home_score:
            return 1
        elif home_score < away_score:
            return -1
        else:
            return 0
    else:
        return 0


def vsin_model_prediction_spread(row):
    if check_data(row, ['away_est_score', 'home_est_score', 'away_money']):
        away_score = float(row['away_est_score'])
        home_score = float(row['home_est_score'])
        away_spread = float(row['away_money']) * -1
        away_diff = away_score - home_score
        if away_diff > away_spread:
            return 1
        elif away_spread < away_diff:
            return -1
        else:
            return 0
    else:
        return 0


def vsin_model_prediction_total(row):
    if check_data(row, ['away_est_score', 'home_est_score', 'Over_line']):
        away_score = float(row['away_est_score'])
        home_score = float(row['home_est_score'])
        pred_total = away_score + home_score
        game_total = float(row['Over_line'])
        if pred_total > game_total:
            return 1
        elif game_total < pred_total:
            return -1
        else:
            return 0
    else:
        return 0


def follow_sharps_spread(row):
    if check_data(row, ['away_spread_handle', 'home_spread_handle']):
        away_handle = float(row['away_spread_handle'])
        home_handle = float(row['home_spread_handle'])
        if away_handle > home_handle:
            return 1
        elif home_handle < away_handle:
            return -1
        else:
            return 0
    else:
        return 0


def fade_sharps_spread(row):
    return -follow_sharps_spread(row)


def follow_sharps_ml(row):
    if check_data(row, ['away_money_handle', 'home_money_handle']):
        away_handle = float(row['away_money_handle'])
        home_handle = float(row['home_money_handle'])
        if away_handle > home_handle:
            return 1
        elif home_handle < away_handle:
            return -1
        else:
            return 0
    else:
        return 0


def fade_sharps_ml(row):
    return -follow_sharps_ml(row)


def follow_sharps_total(row):
    if check_data(row, ['over_total_handle', 'under_total_handle']):
        over_handle = float(row['over_total_handle'])
        under_handle = float(row['under_total_handle'])
        if over_handle > under_handle:
            return 1
        elif under_handle < over_handle:
            return -1
        else:
            return 0
    else:
        return 0


def fade_sharps_total(row):
    return -follow_sharps_total(row)


def follow_public_spread(row):
    if check_data(row, ['away_spread_bets', 'home_spread_bets']):
        away_bets = float(row['away_spread_bets'])
        home_bets = float(row['home_spread_bets'])
        if away_bets > home_bets:
            return 1
        elif home_bets < away_bets:
            return -1
        else:
            return 0
    else:
        return 0


def fade_public_spread(row):
    return -follow_public_spread(row)


def follow_public_ml(row):
    if check_data(row, ['away_money_bets', 'home_money_bets']):
        away_bets = float(row['away_money_bets'])
        home_bets = float(row['home_money_bets'])
        if away_bets > home_bets:
            return 1
        elif home_bets < away_bets:
            return -1
        else:
            return 0
    else:
        return 0


def fade_public_ml(row):
    return -follow_public_ml(row)


def follow_public_total(row):
    if check_data(row, ['over_total_bets', 'under_total_bets']):
        over_bets = float(row['over_total_bets'])
        under_bets = float(row['under_total_bets'])
        if over_bets > under_bets:
            return 1
        elif under_bets < over_bets:
            return -1
        else:
            return 0
    else:
        return 0


def fade_public_total(row):
    return -follow_public_total(row)


def espn_prediction(row):
    if check_data(row, ['team1_pred', 'team2_pred']):
        away_pred = float(row['team1_pred'])
        home_pred = float(row['team2_pred'])
        if away_pred > home_pred:
            return 1
        elif home_pred < away_pred:
            return -1
        else:
            return 0
    else:
        return 0
