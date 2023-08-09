from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib, ssl
from jinja2 import Environment, FileSystemLoader
import redshift_connector

from oauth2client.service_account import ServiceAccountCredentials
import gspread

from config_info import email_account
import config_info


def connect_redshift():
    try:
        conn = redshift_connector.connect(
            host=config_info.host,
            database=config_info.database,
            port=config_info.port,
            user=config_info.user,
            password=config_info.password
        )
        cur = conn.cursor()
        return cur
    except Exception as e:
        print(e)


def execute_sql(sql):
    cur = connect_redshift()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    return result


def connect_worksheet(sheet_name):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name("/Users/linh.ntd/Documents/ggsheet_access.json",
                                                                   scopes)
    gg_sheet = gspread.authorize(credentials)  #authen
    file_ggsheet = gg_sheet.open_by_url(
        "https://docs.google.com/spreadsheets/d/1_DEJZ4pJVeH8RxGojCsOZH7YKpgXCiPXG2bG8_rQASk/edit?usp=sharing")
    sheet = file_ggsheet.worksheet(sheet_name)
    return sheet


def send_email(app_id, app_name, threshold_per, time_loop, total_money_using):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('template.html')
    body = template.render(app_name=app_name, app_id=app_id, threshold_per=threshold_per, time_loop=time_loop,
                           total_money_using=total_money_using)

    message = MIMEMultipart("alternative")
    message['From'] = email_account.email_sender
    message['To'] = email_account.email_receiver
    message['Subject'] = email_account.email_subject.format(app_name)
    message.attach(MIMEText(body, "plain"))

    part1 = MIMEText(body, "html")
    message.attach(part1)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        try:
            server.login(email_account.email_sender, email_account.email_password)
            server.sendmail(email_account.email_sender, email_account.email_receiver, message.as_string())
        except Exception as error:
            print(error)
