def two_way_hcap(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {'team_1_hcap': price0['handicap'],
                'team_1_hcap_odds': price0['american'],
                'team_2_hcap': price1['handicap'],
                'team_2_hcap_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_hcap: {e}")
        return {'team_1_hcap': 'nan', 'team_1_hcap_odds': 'nan', 'team_2_hcap': 'nan',
                'team_2_hcap_odds': 'nan'}

def two_way_12(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {'team_1_ml_odds': price0['american'],
                'team_2_ml_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_ml: {e}")
        return {'team_1_ml_odds': 'nan',
                'team_2_ml_odds': 'nan'}

def three_way_1X2(mkt):
    price0, price1, price2 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price'], mkt['outcomes'][2][
        'price']
    return {'team_1_ml_odds': price0['american'],
            'team_2_ml_odds': price1['american'],
            'draw_ml_odds': price2['american']}

def two_way_OU(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {f'{mkt["outcomes"][0]["description"]}_line': price0['handicap'],
                f'{mkt["outcomes"][0]["description"]}_odds': price0['american'],
                f'{mkt["outcomes"][1]["description"]}_line': price1['handicap'],
                f'{mkt["outcomes"][1]["description"]}_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_OU: {e}")
        try:
            description0 = mkt["outcomes"][0]["description"]
        except Exception:
            description0 = 'nan'
        try:
            description1 = mkt["outcomes"][1]["description"]
        except Exception:
            description1 = 'nan'
        return {f'{description0}_line': 'nan',
                f'{description0}_odds': 'nan',
                f'{description1}_line': 'nan',
                f'{description1}_odds': 'nan'}