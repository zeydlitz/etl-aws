from datetime import date, timedelta
import sqlalchemy as sa
from sqlalchemy.sql import text

pooled_engine = sa.create_engine(
    'redshift+psycopg2://awsuser:25885254Wa@red.ca5unjpxzyhx.eu-west-1.redshift.amazonaws.com:5439/dwh')

sdate = date(2021, 1, 1)  # start date
edate = date(2021, 12, 31)  # end date

if __name__ == "__main__":
    statement = text(
        """INSERT INTO dim.date(datekey , date, year, month, day)
         VALUES(:datekey , :date, :year, :month, :day)""")
    with pooled_engine.connect() as connection:
        for i in range((edate - sdate).days + 1):
            datekey = sdate.strftime('%Y%m%d')
            print(datekey)
            with connection.begin():
                connection.execute(statement,
                                   {"datekey": datekey, "date": sdate, "year": sdate.year,
                                    "month": sdate.month, "day": sdate.day})
            sdate = sdate + timedelta(days=1)
