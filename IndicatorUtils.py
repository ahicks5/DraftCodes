import pandas as pd


def a_sp_follow_public(row):
    # handle bad data
    if pd.isna(row['away_spread_bets']) or pd.isna(row['home_spread_bets']):
        return 0

    if float(row['away_spread_bets']) > 0.5:
        return 1
    elif float(row['away_spread_bets']) < 0.5:
        return -1
    else:
        return 0


def a_sp_follow_sharp(row):
    # handle bad data
    if pd.isna(row['away_spread_handle']) or pd.isna(row['home_spread_handle']):
        return 0

    if float(row['away_spread_handle']) > 0.5:
        return 1
    elif float(row['away_spread_handle']) < 0.5:
        return -1
    else:
        return 0


def a_ml_follow_public(row):
    # handle bad data
    if pd.isna(row['away_ml_bets']) or pd.isna(row['home_ml_bets']):
        return 0

    if float(row['away_ml_bets']) > 0.5:
        return 1
    elif float(row['away_ml_bets']) < 0.5:
        return -1
    else:
        return 0


def a_ml_follow_sharp(row):
    # handle bad data
    if pd.isna(row['away_ml_handle']) or pd.isna(row['home_ml_handle']):
        return 0

    if float(row['away_ml_handle']) > 0.5:
        return 1
    elif float(row['away_ml_handle']) < 0.5:
        return -1
    else:
        return 0


def u_follow_public(row):
    # handle bad data
    if pd.isna(row['total_under_bets']) or pd.isna(row['total_over_bets']):
        return 0

    if float(row['total_under_bets']) > 0.5:
        return 1
    elif float(row['total_under_bets']) < 0.5:
        return -1
    else:
        return 0


def u_follow_sharp(row):
    # handle bad data
    if pd.isna(row['total_under_handle']) or pd.isna(row['total_over_handle']):
        return 0

    if float(row['total_under_handle']) > 0.5:
        return 1
    elif float(row['total_under_handle']) < 0.5:
        return -1
    else:
        return 0


def moneyline_to_probability_1(row):
    # handle bad data
    if pd.isna(row['team_1_ml_odds']) or pd.isna(row['team_2_ml_odds']):
        return 0

    moneyline1 = int(row['team_1_ml_odds'])
    moneyline2 = int(row['team_2_ml_odds'])

    if moneyline1 > 0:
        prob1 = 100 / (moneyline1 + 100)
    else:
        prob1 = abs(moneyline1) / (abs(moneyline1) + 100)

    if moneyline2 > 0:
        prob2 = 100 / (moneyline2 + 100)
    else:
        prob2 = abs(moneyline2) / (abs(moneyline2) + 100)

    # Normalize probabilities to account for the juice
    norm_prob1 = prob1 / (prob1 + prob2)
    norm_prob2 = prob2 / (prob1 + prob2)

    return norm_prob1


def moneyline_to_probability_2(row):
    # handle bad data
    if pd.isna(row['team_1_ml_odds']) or pd.isna(row['team_2_ml_odds']):
        return 0

    moneyline1 = int(row['team_1_ml_odds'])
    moneyline2 = int(row['team_2_ml_odds'])

    if moneyline1 > 0:
        prob1 = 100 / (moneyline1 + 100)
    else:
        prob1 = abs(moneyline1) / (abs(moneyline1) + 100)

    if moneyline2 > 0:
        prob2 = 100 / (moneyline2 + 100)
    else:
        prob2 = abs(moneyline2) / (abs(moneyline2) + 100)

    # Normalize probabilities to account for the juice
    norm_prob1 = prob1 / (prob1 + prob2)
    norm_prob2 = prob2 / (prob1 + prob2)

    return norm_prob2


def a_ml_follow_espn(row):
    # handle bad data
    if pd.isna(row['team1_pred']) or pd.isna(row['team2_pred']) or pd.isna(row['a_bov_implied_ml_percent']) or pd.isna(row['h_bov_implied_ml_percent']):
        return 0

    if float(row['team1_pred']) > float(row['a_bov_implied_ml_percent']):
        return 1
    elif float(row['team2_pred']) > float(row['h_bov_implied_ml_percent']):
        return -1
    else:
        return 0
