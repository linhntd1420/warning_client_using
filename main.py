from sql import extract_sql
from service import send_email, execute_sql, connect_worksheet
import pandas as pd
from datetime import date, timedelta


def main_func():
    sheet_config = connect_worksheet("config")
    config_rule = pd.DataFrame(sheet_config.get_all_records())
    for index in config_rule.drop_duplicates(subset=["app_id"]).index:  # for distinct app_id
        app_id = config_rule['app_id'][index]
        current_using = execute_sql(extract_sql.format(APP_IDS=app_id, USING_TIME=config_rule['using_time'][index]))[0][0]

        app_id_rule = config_rule[config_rule['app_id'] == app_id]
        for threshold_value in app_id_rule.sort_values(by='threshold_value', ascending=False)['threshold_value']:

            if current_using >= threshold_value:
                send_email(app_id, app_id_rule['app_name'].values[0],
                           app_id_rule[app_id_rule['threshold_value'] == threshold_value][
                               'threshold_percentage'].values[0],
                           ''.join([app_id_rule['using_time'].values[0], ' đến ', str(date.today())]), current_using)
                if app_id_rule[app_id_rule['threshold_value'] == threshold_value][
                    'threshold_percentage'].values[0] == '100%':
                    list_of_using_time = sheet_config.findall(config_rule['using_time'][index])
                    for using_time in list_of_using_time:
                        using_time.value = str(date.today() + timedelta(days=1))
                        sheet_config.update_cells(list_of_using_time)
                break

if __name__ == '__main__':
    main_func()
