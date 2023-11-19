import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote_plus

import pandas as pd
from python3.scripts.variables import get_value
from sqlalchemy import create_engine

email_login = get_value("email_login")
email_password = get_value("email_password")
base_login = get_value("base_login")
base_password = get_value("base_password")

recipients = "active directory group"
file = "Logs.xlsx"
query = r"""
        SELECT 
        	 [UserName] 
        	,[ReportPath]
        	,[VisitDate] = CONCAT(CONVERT(NVARCHAR(16), [TimeStart], 126)," ", CONVERT(NVARCHAR(16), [TimeEnd], 126))
        	,[VisitAmount] = COUNT(DISTINCT CONCAT(CONVERT(NVARCHAR(16), [TimeStart], 126),"-",[UserName]))
        FROM [ReportServer].[dbo].[ExecutionLog2]
        WHERE SUBSTRING([ReportPath], 26, 4) IN("6102", "6170", "6171", "6173", "6175", "6176", "6177", "6178")
        OR SUBSTRING([ReportPath], 32, 4) IN("6173", "6178")
		OR SUBSTRING([ReportPath], 67, 4) IN("6136")
        --AND [TimeStart] BETWEEN "2023-03-01" AND "2023-03-20"
        AND [Format] = "PBIX"
        GROUP BY 
        	 [UserName] 
        	,[ReportPath] 
        	,CONCAT(CONVERT(NVARCHAR(16), [TimeStart], 126), " ", CONVERT(NVARCHAR(16), [TimeEnd], 126))
        ORDER BY 2 DESC, 3 DESC
"""


def get_engine():
    conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRS2;DATABASE=ReportServer;UID="+base_login+";PWD="+base_password+";Trusted_Connection=yes"
    quoted = quote_plus(conn)
    new_con = "mssql+pyodbc:///?odbc_connect={}".format(quoted)
    engine = create_engine(new_con, fast_executemany=True)
    return engine


def write_dataframe(query):
    conn = get_engine()
    sql_query = pd.read_sql_query(query, conn)
    dataframe = pd.DataFrame(sql_query)
    return dataframe


def sent_email(email_login, subject, text, file_name, recipients):
    msg = MIMEMultipart()
    msg["From"] = email_login
    msg["To"] = recipients
    msg["Subject"] = subject
    msg.attach(MIMEText(text, "plain"))
    msg.attach(file_name)

    email_server = smtplib.SMTP_SSL("mail.casp.tech", 430)
    email_server.ehlo(email_login)
    email_server.login(email_login, email_password)
    email_server.auth_plain()
    email_server.send_message(msg)
    email_server.quit()


def main(file, email_login, recipients, query):
    dataframe = write_dataframe(query)
    dataframe.to_excel(file, index=False)
    attachment = open(file, "rb")
    file_name = MIMEApplication(attachment.read(), subtype="xlsx")
    file_name.add_header("Content-Disposition", "attachment", filename=file)
    attachment.close()

    sent_email(email_login, "Файл с логами SRS", "Данное письмо сгенирировано автоматически!", file_name, recipients)


main(file, email_login, recipients, query)
