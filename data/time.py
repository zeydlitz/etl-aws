from datetime import timedelta
import sqlalchemy as sa
from sqlalchemy.sql import text

pooled_engine = sa.create_engine(
    'redshift+psycopg2://awsuser:25885254Wa@red.ca5unjpxzyhx.eu-west-1.redshift.amazonaws.com:5439/dwh')

if __name__ == "__main__":
    statement = text(
        """INSERT INTO dim.time(timekey ,time, hour, minute)
         VALUES(:timekey ,:time, :hour, :minute)""")
    with pooled_engine.connect() as connection:
        for i in range(1440):
            time = i * timedelta(minutes=1)
            time_str = f"0{str(time)}"
            with connection.begin():
                print(time_str, i)
                if i > 599:
                    connection.execute(statement,
                                   {"timekey": f"{time_str[1:3]}{time_str[4:6]}", "time": time, "hour": time_str[1:3],
                                    "minute": time_str[4:6]})
                else:
                    connection.execute(statement,
                                   {"timekey": f"0{time_str[:2]}{time_str[3:5]}", "time": time, "hour": f"0{time_str[:2]}",
                                    "minute": time_str[3:5]})
