import pandas as pd


PATHS_LOCAL = {
    'bovada_data': 'Bovada_Data.csv',
    'bovada_links': 'Bovada_Sport_Links.csv',
    'espn_data': 'ESPN_Data.csv',
    'ind_data': 'Indicator_Data.csv',
    'sport_ref': 'Sport_Reference.csv',
    'team_ref': 'Team_Reference.csv',
}

AWS_PATH = '/var/www/html/Website/'


def load_previous_data():
    data_dict = {}

    try:
        for file in PATHS_LOCAL:
            # first try for aws location
            try:
                data_dict[file] = pd.read_csv(AWS_PATH + PATHS_LOCAL[file])
            except UnicodeDecodeError:
                data_dict[file] = pd.read_csv(AWS_PATH + PATHS_LOCAL[file], encoding='ISO-8859-1')

    except:
        for file in PATHS_LOCAL:
            # first try for aws location
            try:
                data_dict[file] = pd.read_csv(PATHS_LOCAL[file])
            except UnicodeDecodeError:
                data_dict[file] = pd.read_csv(PATHS_LOCAL[file], encoding='ISO-8859-1')

    return data_dict


if __name__ == '__main__':
    data_dict = load_previous_data()
    print(data_dict)
